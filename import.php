#!/usr/bin/php
<?php
global $server, $dbname, $mysql_user, $mysql_password;
$url = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/";

include_once "vendor/autoload.php";
include_once "inc/connection.php";

use KitsuneTech\Velox\Database\Connection;
use KitsuneTech\Velox\Database\Procedures\{Query, PreparedStatement, StatementSet, Transaction};
use function KitsuneTech\Velox\Database\oneShot;

/**
 * Simply spits out usage instructions and ends the script.
 *
 * @return void
 */
function printUsage(): void {
    echo "Usage: ./import.php [resume type flag] --hs <hardware/software CSV> --sv <software/vehicle CSV> --hh <hardware/Hollander CSV> --sh <software/Hollander CSV> <vehicle info flag>\n";
    echo "\n";
    echo "Vehicle info flags:\n";
    echo "    --use-vin     The software/vehicle CSV contains VINs.\n";
    echo "    --use-spec    The software/vehicle CSV contains vehicle specifications.\n";
    echo "\n";
    echo "Resume type flags:\n";
    echo "    --resume-vpic           Resume pending vPIC API calls\n";
    echo "    --resume-processing     Skip any pending vPIC API calls and process results that have already arrived\n";
    echo "\n";
    die ("See README.md for more detailed information.\n\n");
}

/**
 * Parses the CSV file at $path into a two-dimensional array, taking the first $columnCount columns.
 *
 * *Note: this CSV is assumed to have headings in the first row, which is ignored; the results start at row 2 of the CSV.*
 *
 * @param string $path The path of the CSV file to be parsed
 * @param int $columnCount The number of columns to be retrieved
 * @return array The results as a two-dimensional array
 */
function parseCSV(string $path, int $columnCount) : array {
    $contents = [];
    if (!file_exists($path)){
        die("Error: $path does not exist.");
    }
    if (!$pointer = fopen($path, "r")) {
        die("Error: $path could not be opened. Possibly a permission issue?\n\n");
    }
    while ($line = fgetcsv($pointer)){
        $contents[] = array_slice($line,0,$columnCount);
    }
    array_shift($contents);
    fclose($pointer);
    return $contents;
}

/* Define initial variables */
$csvPaths = [];
$info_type = null;
$resume_type = null;
$hsPointer = null;
$svPointer = null;
$hsContents = [];
$svContents = [];
$verbose = false;

/* Get flags from CLI */
$arguments = $argv;
array_shift($arguments);
for ($i=0; $i < count($arguments); $i++) {
    switch ($arguments[$i]) {
        case '--hs':
        case '--sv':
        case '--hh':
        case '--sh':
            //The values for each flag are file paths to be assigned to variables having the name of that flag.
            //e.g. --hs /path/to/file  -->  $hs = "/path/to/file"
            $flag = substr($arguments[$i], 2);
            $csvPaths[$flag] = $arguments[$i+1];
            $i++;
            break;
        case '--use-vin':
        case '--use-spec':
            if (!$info_type) {
                $info_type = $arguments[$i];
            }
            else {
                echo "--use-vin and --use-spec cannot both be specified.\n\n";
                printUsage();
            }
            break;
        case '--verbose':
            $verbose = true;
            break;
        case '--resume-vpic':
        case '--resume-processing':
            if (!$resume_type) {
                $resume_type = $arguments[$i];
            }
            else {
                echo "--resume-vpic and --resume-processing cannot both be specified.\n\n";
                printUsage();
            }
            break;
    }
}

//If a resume type is specified, skip straight to the appropriate section
switch ($resume_type){
    case '--resume-vpic':
        goto resume_vpic;
    case '--resume-processing':
        goto resume_processing;
}

if (count($csvPaths) === 0) {
    echo "At least one CSV file must be specified for import.\n\n";
    printUsage();
}
if (isset($csvPaths['sv']) && !$info_type){
    echo "Either --use-vin or --use-spec is required if a software/vehicle CSV file is specified.\n\n";
    printUsage();
}

//Initialize the database connection
$conn = new Connection($server,$dbname,$mysql_user,$mysql_password);

//Insert all hardware/software numbers
if (isset($csvPaths['hs'])){
    if ($verbose) echo "Inserting hardware/software into t_hs.\n";
    //Get the CSV data
    $hsContents = parseCSV($csvPaths['hs'], 2);
    if ($verbose) echo "Row count:" . count($hsContents)."\n";

    //First dump the CSV dataset into a temporary table
    oneShot(new Query($conn, "CREATE TEMPORARY TABLE t_hs (`inventory_no` VARCHAR(255) NOT NULL,`mfr_software_no` VARCHAR(255) NOT NULL)"));

    //INSERT IGNORE here, along with the NOT NULL constraints, sanitize the data for rows missing data; this is thrown out in the process of the INSERT
    $hsInsert = new PreparedStatement($conn, "INSERT IGNORE INTO t_hs (inventory_no, mfr_software_no) VALUES(?,?)");
    for ($i = 0; $i < count($hsContents); $i++){
        if (in_array('',$hsContents[$i])) continue;     //Skip any lines that have empty strings for either value
        $hsInsert->addParameterSet($hsContents[$i]);
    }
    $hsInsert();

    //Perform both inserts as an atomic transaction using the temporary table as a source
    $hsTransaction = new Transaction($conn);
    $hsQueries = [
        new Query($conn,"INSERT IGNORE INTO inventory (inventory_no) SELECT DISTINCT inventory_no FROM t_hs"),
        new Query($conn, "INSERT IGNORE INTO software (inventory_id, mfr_software_no) SELECT inventory_id, mfr_software_no FROM t_hs INNER JOIN inventory USING (inventory_no)")
    ];
    $hsTransaction->begin();
    foreach ($hsQueries as $query){
        $query();
    }
    $hsTransaction->commit();
}

//Insert software/Hollander matches (if provided)
if (isset($csvPaths['sh'])){
    if ($verbose) echo "Parsing software/Hollander CSV.\n";
    $shContents = parseCSV($csvPaths['sh'], 2);
    if ($verbose) echo "Row count:" . count($shContents)."\n";
    if ($verbose) echo "Creating software/Hollander temp table.\n";
    oneShot(new Query($conn, "CREATE TEMPORARY TABLE t_sh (`mfr_software_no` VARCHAR(255) NOT NULL,`hollander_no` VARCHAR(255) NOT NULL)"));
    if ($verbose) echo "Inserting CSV data into software/Hollander temp table.\n";
    $shInsert = new PreparedStatement($conn, "INSERT IGNORE INTO t_sh (mfr_software_no, hollander_no) VALUES(?,?)");
    for ($i = 0; $i < count($shContents); $i++){
        if (in_array('',$shContents[$i])) continue;     //Skip any lines that have empty strings for either value
        $shInsert->addParameterSet($shContents[$i]);
    }
    $shInsert();
    if ($verbose) echo "Inserting any new Hollander numbers into hollander table.\n";
    oneShot(new Query($conn,"INSERT IGNORE INTO hollander (hollander_no) SELECT DISTINCT hollander_no FROM t_sh"));
    if ($verbose) echo "Adding Hollander/software matches to hollander_software_map table.\n";
    oneShot(new Query($conn,"INSERT IGNORE INTO hollander_software_map (software_id, hollander_id) ".
                                    "SELECT software_id, hollander_id FROM t_sh ".
                                    "INNER JOIN software USING (mfr_software_no)".
                                    "INNER JOIN hollander USING (hollander_no)"));
}

//Insert hardware/Hollander matches (if provided)
if (isset($csvPaths['hh'])){
    if ($verbose) echo "Parsing hardware/Hollander CSV.\n";
    $hhContents = parseCSV($csvPaths['hh'], 2);
    if ($verbose) echo "Row count:" . count($hhContents)."\n";
    if ($verbose) echo "Creating hardware/Hollander temp table.\n";
    oneShot(new Query($conn, "CREATE TEMPORARY TABLE t_hh (`inventory_no` VARCHAR(255) NOT NULL,`hollander_no` VARCHAR(255) NOT NULL)"));
    if ($verbose) echo "Inserting CSV data into hardware/Hollander temp table.\n";
    $hhInsert = new PreparedStatement($conn, "INSERT IGNORE INTO t_hh (inventory_no, hollander_no) VALUES(?,?)");
    for ($i = 0; $i < count($hhContents); $i++){
        if (in_array('',$hhContents[$i])) continue;     //Skip any lines that have empty strings for either value
        $hhInsert->addParameterSet($hhContents[$i]);
    }
    $hhInsert();
    if ($verbose) echo "Inserting any new Hollander numbers into hollander table.\n";
    oneShot(new Query($conn,"INSERT IGNORE INTO hollander (hollander_no) SELECT DISTINCT hollander_no FROM t_hh"));
    if ($verbose) echo "Adding Hollander/software matches to hollander_inventory_map table.\n";
    oneShot(new Query($conn,"INSERT IGNORE INTO inventory_hollander_map (inventory_id, hollander_id) ".
                                    "SELECT inventory_id, hollander.hollander_id FROM t_hh ".
                                    "INNER JOIN inventory USING (inventory_no)".
                                    "INNER JOIN hollander USING (hollander_no)"));
}

if (!isset($csvPaths['sv'])) {
    die("Complete.\n\n");
}

// ----- Everything from here on pertains only to vehicle/software matches. ----- //

$colCount = match ($info_type){
    '--use-vin' => 2,
    '--use-spec' => 8
};
$svContents = parseCSV($csvPaths['sv'],$colCount);
$svRowCount = count($svContents);
if ($verbose) echo "Row count: $svRowCount\n";

$svTableSQL = "CREATE TEMPORARY TABLE IF NOT EXISTS t_sv (`mfr_software_no` VARCHAR(255) NOT NULL, ";
$svInsertSQL = "INSERT IGNORE INTO t_sv (mfr_software_no, ";
switch ($info_type) {
    case '--use-vin':
        $svTableSQL .= "`vin` VARCHAR(17) NOT NULL, ";
        $svInsertSQL .= "vin)";
        $colCount = 2;
        break;
    case '--use-spec':
        $svTableSQL .= "`make_name` VARCHAR(255) NOT NULL, ";
        $svTableSQL .= "`model_name` VARCHAR(255) NOT NULL, ";
        $svTableSQL .= "`model_year` SMALLINT(5) UNSIGNED NOT NULL, ";
        $svTableSQL .= "`engine_displacement` DECIMAL(3,1) NOT NULL, ";
        $svTableSQL .= "`engine_type` VARCHAR(255) NOT NULL DEFAULT 'Gasoline', ";
        $svTableSQL .= "`vehicle_trim` VARCHAR(100), ";
        $svTableSQL .= "`vehicle_series` VARCHAR(100), ";
        $svInsertSQL .= "make_name, model_name, model_year, engine_displacement, engine_type, vehicle_trim, vehicle_series)";
        $colCount = 8;
        break;
    default:
        die("Error -- the proper vehicle info flag was not found.\n\n");
}
//We'll populate the following two later, and the results will be inserted into vehicle_software_map
$svTableSQL .= "`software_id` INT(11) UNSIGNED, ";
$svTableSQL .= "`vehicle_id` INT(11) UNSIGNED)";

//Creates an "?,?,?" string of the appropriate length
$placeholderArray = [];
for ($i=0; $i<$colCount; $i++){
    $placeholderArray[$i] = "?";
}
$placeholders = implode(',',$placeholderArray);
$svInsertSQL .= "VALUES ($placeholders)";

if ($verbose) echo "Creating software/vehicle temp table.\n";
oneShot(new Query($conn, $svTableSQL));

if ($verbose) echo "Inserting CSV data into software/vehicle temp table.\n";
$svInsert = new PreparedStatement($conn,$svInsertSQL);
for ($i = 0; $i < $svRowCount; $i++){
    $rowHasData = false;
    for ($j = 0; $j < count($svContents[$i]); $j++){
        if (!isset($svContents[$i][$j]) || $svContents[$i][$j] == '') {
            $svContents[$i][$j] = null;
        }
        else {
            $rowHasData = true;
        }
    }
    if ($info_type == '--use-vin'){
        //Skip any invalid VINs
        if (!$svContents[$i][1] ||(preg_match("/^[A-HJ-NPR-Z\d]{8}[\dX\_][A-HJ-NPR-Z\d]{1}/i",$svContents[$i][1])) != 1) continue;
    }
    if ($rowHasData) $svInsert->addParameterSet($svContents[$i]);
}

$svInsert();

if ($verbose) echo "Matching software numbers with existing records.\n";
oneShot(new Query($conn,"UPDATE t_sv INNER JOIN software USING (mfr_software_no) SET t_sv.software_id = software.software_id"));

//t_sv vehicle_id matching will depend on whether we're working with VINs or vehicle specs.
switch ($info_type){
    case "--use-vin":
        if ($verbose) echo "Updating software/vehicle temp table with VIN matches from existing data. (This might take a while...)\n";
        $vinSQL =
            "UPDATE t_sv INNER JOIN (".
                "SELECT vehicle_id, CONCAT(wmi_code,vds_code,'_',year_digit,'%') AS vin_pattern ".
                "FROM l_WMI ".
                "INNER JOIN l_VDS USING (wmi_id) ".
                "INNER JOIN vehicle_identities USING (vds_id) ".
            ") AS vins ON t_sv.vin LIKE vins.vin_pattern SET t_sv.vehicle_id = vins.vehicle_id";
        $svUpdate = new PreparedStatement($conn,$vinSQL);
        $svUpdate();
        break;
    case "--use-spec":
        if ($verbose) echo "Updating software/vehicle temp table with vehicle spec matches from existing data.\n";

        $specSQL = "UPDATE t_sv ".
                    "INNER JOIN vehicles ON ".
                        "t_sv.make_name = vehicles.make_name ".
                        "AND t_sv.model_name = vehicles.model_name ".
                        "AND t_sv.model_year = vehicles.model_year ".
                        "AND t_sv.engine_displacement = vehicles.engine_displacement ".
                        "AND (t_sv.vehicle_series = vehicles.vehicle_series OR (t_sv.vehicle_series IS NULL AND vehicles.vehicle_series IS NULL)) ".
                        "AND (t_sv.vehicle_trim = vehicles.vehicle_trim OR (t_sv.vehicle_trim IS NULL AND vehicles.vehicle_trim IS NULL)) ".
                    "SET t_sv.vehicle_id = vehicles.vehicle_id WHERE t_sv.vehicle_id IS NULL";
        oneShot(new PreparedStatement($conn,$specSQL));
        $inserts = [];
        $inserts[] = "INSERT IGNORE INTO l_makes (make_name) SELECT DISTINCT make_name FROM t_sv LEFT JOIN l_makes using (make_name) WHERE make_id is null";
        $inserts[] = "INSERT IGNORE INTO l_models (make_id, model_name) SELECT DISTINCT l_makes.make_id, t_sv.model_name FROM t_sv INNER JOIN l_makes using (make_name) LEFT JOIN l_models ON l_makes.make_id = l_models.make_id AND t_sv.model_name = l_models.model_name WHERE model_id IS NULL";
        $inserts[] = "INSERT IGNORE INTO l_engine_types (engine_type_name) SELECT DISTINCT engine_type FROM t_sv LEFT JOIN l_engine_types ON t_sv.engine_type = l_engine_types.engine_type_name WHERE engine_type_id IS NULL";
        $inserts[] = "INSERT INTO vehicle_identities (make_id, model_year, engine_displacement, engine_type_id, vehicle_series, vehicle_trim) ".
            "SELECT make_id, model_year, engine_displacement, engine_type_id, vehicle_series, vehicle_trim ".
            "FROM t_sv ".
            "INNER JOIN l_makes USING (make_name) ".
            "INNER JOIN l_models ON l_makes.make_id = l_models.make_id AND t_sv.model_name = l_models.model_name ".
            "INNER JOIN l_engine_types ON t_sv.engine_type = l_engine_types.engine_type_name ".
            "WHERE t_sv.vehicle_id IS NULL";
        for($i=0; $i<count($inserts); $i++){
            oneShot(new Query($conn,$inserts[$i]));;
        }
        oneShot(new PreparedStatement($conn,$specSQL));
        break;
}


if ($verbose) echo "Updating vehicle_software_map with matches on existing data.\n";
$insertVSM_SQL = "INSERT IGNORE INTO vehicle_software_map (vehicle_id, software_id) ".
                    "SELECT t_sv.vehicle_id, t_sv.software_id FROM t_sv ".
                    "LEFT JOIN vehicle_software_map ON vehicle_software_map.vehicle_id = t_sv.vehicle_id AND vehicle_software_map.software_id = t_sv.software_id ".
                    "WHERE ".
                    "t_sv.vehicle_id IS NOT NULL AND t_sv.software_id IS NOT NULL AND ".
                    "vehicle_software_map.vehicle_id IS NULL AND vehicle_software_map.software_id IS NULL";

$insertVSM = new Query($conn, $insertVSM_SQL);

$insertVSM();

if ($info_type == '--use-spec'){
    die("Complete. \n");
}

if ($verbose) echo "All possible matching done from existing data in NIS. Preparing for vPIC API calls for unmatched VINs...\n";


//Add a non-temporary table to hold unmatched VINs
// (non-temporary because it can take a while to run all the vPIC API calls and we want to be able to resume if the script ends unexpectedly)

if ($verbose) echo "Creating t_unmatched_vehicles table if it doesn't already exist.\n";
$createUnmatchedVehicleTable_SQL = "CREATE TABLE IF NOT EXISTS t_unmatched_vehicles (".
    "`vin_pattern` VARCHAR(17) NOT NULL, ".
    "`vds_id` INT(11) UNSIGNED, ".
    "`year_digit` VARCHAR(2), ".
    "`engine_displacement` DECIMAL(3,1), ".
    "`make_name` VARCHAR(255), ".
    "`model_name` VARCHAR(255), ".
    "`model_year` SMALLINT(5) UNSIGNED, ".
    "`expected_year` SMALLINT(5) UNSIGNED, ".
    "`vehicle_series` VARCHAR(100), ".
    "`vehicle_trim` VARCHAR(100), ".
    "`engine_type_name` VARCHAR(255),".
    "`matched` BOOL DEFAULT FALSE, ".
    "`vehicle_id` INT(11) UNSIGNED, ".
    "UNIQUE (vin_pattern)".
    ")";

oneShot(new Query($conn,$createUnmatchedVehicleTable_SQL));
oneShot(new Query($conn, "TRUNCATE TABLE t_unmatched_vehicles"));

if ($verbose) echo "Adding missing WMI and VDS records.\n";
oneShot(new Query($conn,"INSERT IGNORE INTO l_WMI (wmi_code) SELECT DISTINCT LEFT(vin,3) FROM t_sv"));
oneShot(new Query($conn,"INSERT IGNORE INTO l_VDS (vds_code, wmi_id) SELECT DISTINCT SUBSTRING(vin,4,5), wmi_id FROM t_sv ".
                                "INNER JOIN l_WMI ON LEFT(vin,3) = l_WMI.wmi_code"));

if ($verbose) echo "Populating t_unmatched_vehicles with necessary data from t_sv and VIN component tables.\n";
$populateUnmatchedVehicle_SQL = "INSERT IGNORE INTO t_unmatched_vehicles (vin_pattern, vds_id, year_digit, expected_year) ".
    "SELECT CONCAT(first8,'_',digit,'%'), vds_id, digit, if(substring(first8,7,1) regexp '[0-9]', sequence, sequence + 30)".
    "FROM (SELECT DISTINCT LEFT(vin,8) as first8 FROM t_sv) AS partialVin ".
    "LEFT JOIN l_WMI ON l_WMI.wmi_code = LEFT(first8,3) ".
    "LEFT JOIN l_VDS ON l_WMI.wmi_id = l_VDS.wmi_id AND l_VDS.vds_code = SUBSTRING(first8,4,5) ".
    "CROSS JOIN l_yearDigits ";

oneShot(new Query($conn,$populateUnmatchedVehicle_SQL));

if ($verbose) echo "Creating t_unmatched_software table if it doesn't already exist.\n";
//Do the same thing for software; after this we are no longer dependent on t_sv to complete the job
$createUnmatchedSoftwareTable_SQL = "CREATE TABLE IF NOT EXISTS t_unmatched_software (".
    "`vin_pattern` VARCHAR(17) NOT NULL, ".
    "`software_id` INT(11) UNSIGNED, ".
    "`matched` BOOL DEFAULT FALSE)";

oneShot(new Query($conn,$createUnmatchedSoftwareTable_SQL));

if ($verbose) echo "Populating t_unmatched_software with necessary data from t_sv.\n";
$populateUnmatchedSoftware_SQL = "INSERT INTO t_unmatched_software (vin_pattern, software_id) ".
    "SELECT DISTINCT CONCAT(LEFT(vin,8),'_',SUBSTRING(vin,10,1),'%'), software_id ".
    "FROM t_sv ".
    "WHERE software_id IS NOT NULL";

oneShot(new Query($conn,$populateUnmatchedSoftware_SQL));
oneShot(new Query($conn, "TRUNCATE TABLE t_unmatched_software"));

//If the script dies during the vPIC calls, we can restart the process here with the --resume-vpic flag
resume_vpic:

if (!isset($conn)) $conn = new Connection($server,$dbname,$mysql_user,$mysql_password);

if ($verbose) echo "Batching unmatched VINs from t_unmatched_vehicles.\n";
$unmatchedVins = oneShot(new Query($conn,"SELECT vin_pattern FROM t_unmatched_vehicles WHERE matched = FALSE"))[0];
$unmatchedVinCount = count($unmatchedVins);
$batches = [];
$currentBatch = [];

//Build batches of 50
for ($i=0; $i<$unmatchedVinCount; $i++){
	$currentBatch[] = $unmatchedVins[$i];
	if (count($currentBatch) == 50 || $i == $unmatchedVinCount - 1){
		$batches[] = $currentBatch;
		$currentBatch = [];
	}
}

$updateCriteria = [];
$batchCount = count($batches);
$totalValid = 0;
if ($verbose) echo "$batchCount batches created. Beginning vPIC API calls...\n";
for ($i=0; $i<$batchCount; $i++){
	//Concatenate to string
	$batchStr = implode(";",array_column($batches[$i],"vin_pattern"));

	//Send to VPIC
	$request = curl_init($url);
	curl_setopt($request,CURLOPT_POST,true);
	$postdata = http_build_query(["format"=>"json","data"=>$batchStr]);
	curl_setopt($request, CURLOPT_POSTFIELDS, $postdata);
	curl_setopt($request, CURLOPT_RETURNTRANSFER, true);
	echo "Sending batch ".($i+1)." of $batchCount...\n";
	$response = curl_exec($request);
	if (!$response){
		die ("Request failed.");
	}
	$valid = 0;
	$responseObj = json_decode($response);
    $updateUnmatched = new StatementSet($conn,"UPDATE IGNORE t_unmatched_vehicles SET <<values>> WHERE <<condition>>",Query::QUERY_UPDATE);
	for ($j=0; $j<count($batches[$i]); $j++){
		$vinResponse = $responseObj->Results[$j];
		if ($vinResponse->Model){
            $updateCriteria[] = [
                "values"=> [
                    "engine_displacement"=>$vinResponse->DisplacementL,
                    "make_name"=>$vinResponse->Make,
                    "model_name"=>$vinResponse->Model,
                    "model_year"=>$vinResponse->ModelYear,
                    "vehicle_series"=>$vinResponse->Series,
                    "vehicle_trim"=>$vinResponse->Trim,
                    "engine_type_name"=>$vinResponse->FuelTypePrimary,
                    "matched"=>1
                ],
                "where"=> [["vin_pattern"=>["=",$batches[$i][$j]['vin_pattern']]]]
            ];
			$valid++;
		}
	}
    $totalValid += $valid;
    if ($verbose) echo "Batch ".($i+1)." returned $valid valid vehicles.\n\n";

    //Update the rows for this batch and clear the array for the next iteration
    if ($valid > 0) {
        $updateUnmatched->addCriteria($updateCriteria);
        $updateUnmatched();
    }
    $updateCriteria = [];
}

if ($verbose) echo "$totalValid valid VIN patterns found in $unmatchedVinCount requests.\n\n";

if ($verbose) echo "*** vPIC server requests completed. Processing results... ***\n\n";

//If the script dies during the vPIC calls and we just want to process what didn't make it, we can skip here with --resume-processing
resume_processing:

if (!isset($conn)) $conn = new Connection($server,$dbname,$mysql_user,$mysql_password);

//--------------------------------------------//

//Clear out any rows that were not matched by vPIC
oneShot(new Query($conn,"DELETE FROM t_unmatched_vehicles WHERE matched = 0"));

if ($verbose) echo "Retrieving vPIC-matched vehicles from t_unmatched_vehicles.\n";
$responseArray = oneShot(new Query($conn,"SELECT * FROM t_unmatched_vehicles WHERE matched = 1 AND make_name <> '' AND model_name <> ''"));
$responseCount = count($responseArray);

if ($verbose) echo "Adjusting year increments for vds_id's.\n\n";
oneShot(new Query($conn, "UPDATE l_VDS INNER JOIN t_unmatched_vehicles USING (vds_id) SET year_increment = if(t_unmatched_vehicles.model_year < 2010, 0, 30) where t_unmatched_vehicles.model_year <> t_unmatched_vehicles.expected_year"));

//--------------------------------------------//

if ($verbose) echo "Synchronizing l_makes...\n\n";

$makeInsert = new PreparedStatement($conn, "INSERT IGNORE INTO l_makes (make_name) VALUES (:make_name)");

//vPIC sends makes in all upper-case, so we need to convert them to title case to make them consistent with what we use.
array_walk($responseArray,function(&$elem, $key){ if ($elem['make_name']) $elem['make_name'] = ucwords(strtolower($elem['make_name'])); });

$unmatchedMakes = array_values(array_unique(array_column($responseArray,"make_name")));
foreach ($unmatchedMakes as $make){
    if ($make) $makeInsert->addParameterSet(["make_name"=>$make]);
}
$makeInsert();

//--------------------------------------------//

if ($verbose) echo "Synchronizing l_models...\n\n";

$modelInsertSQL = "INSERT INTO l_models (make_id, model_name) SELECT DISTINCT make_id, model_name FROM t_unmatched_vehicles ".
    "INNER JOIN l_makes USING (make_name) ".
    "LEFT JOIN l_models USING (make_id, model_name) WHERE l_models.model_id IS NULL";
$modelInsert = new PreparedStatement($conn, $modelInsertSQL);

//--------------------------------------------//

if ($verbose) echo "Synchronizing l_engine_types...\n";

$types = array_values(array_unique(array_filter(array_column($responseArray,"engine_type_name"))));

$engineTypeInsertSQL = "INSERT INTO l_engine_types (engine_type_name) SELECT DISTINCT engine_type_name FROM t_unmatched_vehicles".
    "LEFT JOIN l_engine_types USING (engine_type_name) ".
    "WHERE l_engine_types.engine_type_id IS NULL ".
    "AND engine_type_name <> '' AND engine_type_name IS NOT NULL"
;
$engineTypeInsert = new PreparedStatement($conn, $engineTypeInsertSQL);

//--------------------------------------------//
// Insert into vehicle_identities. vds_id and year_digit are paired as a unique key. model_id is required. vehicle_trim, vehicle_series, engine_displacement, and engine_type_id are optional.
// engine_type_id defaults to 1 (gasoline). 2 is diesel.
if ($verbose) echo "Inserting vehicle identities.\n";

$identitiesDataSQL = "INSERT IGNORE INTO vehicle_identities (vds_id, model_id, vehicle_trim, vehicle_series, engine_displacement, year_digit, engine_type_id) ".
    "SELECT vds_id, model_id, vehicle_trim, vehicle_series, engine_displacement, year_digit, engine_type_id ".
    "FROM t_unmatched_vehicles ".
    "INNER JOIN l_makes USING (make_name) ".
    "INNER JOIN l_models USING (make_id, model_name) ".
    "LEFT JOIN l_engine_types USING (engine_type_name) ";

oneShot(new PreparedStatement($conn, $identitiesDataSQL));

// After vehicle identities are created //

if ($verbose) echo "Matching new vehicle_ids to t_unmatched_vehicles.\n";
oneShot(new PreparedStatement($conn,
        "UPDATE t_unmatched_vehicles ".
        "INNER JOIN (".
            "SELECT vehicle_id, CONCAT(wmi_code,vds_code,'_',year_digit,'%') AS vin_pattern ".
            "FROM l_WMI ".
            "INNER JOIN l_VDS USING (wmi_id) ".
            "INNER JOIN vehicle_identities USING (vds_id)".
        ") AS currentPatterns USING (vin_pattern) ".
        "SET t_unmatched_vehicles.vehicle_id = currentPatterns.vehicle_id"));

if ($verbose) echo "Adding new records in vehicle_software_map for vPIC-matched vehicles and software.\n";
oneShot(new Query($conn,
    "INSERT IGNORE INTO vehicle_software_map (vehicle_id, software_id) ".
        "SELECT vehicle_id, software_id ".
        "FROM t_unmatched_vehicles ".
        "INNER JOIN t_unmatched_software USING (vin_pattern)"
));

if ($verbose) echo "All matching complete. Removing matched records from t_unmatched_vehicles and t_unmatched_software.\n";
//Finally, delete all matched rows from the t_ tables (and delete the tables entirely if all rows are matched)
oneShot(new Query($conn,
   "DELETE t_unmatched_software FROM t_unmatched_software ".
       "INNER JOIN t_unmatched_vehicles USING (vin_pattern)".
       "INNER JOIN vehicle_software_map USING (vehicle_id, software_id)"
));

oneShot(new Query($conn,
    "DELETE FROM t_unmatched_vehicles WHERE matched = TRUE"
));

die("Complete.\n\n");
