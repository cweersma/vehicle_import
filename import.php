#!/usr/bin/php
<?php
global $server, $dbname, $mysql_user, $mysql_password;
$url = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/";

include_once "vendor/autoload.php";
include_once "inc/connection.php";

use KitsuneTech\Velox\Database\Connection;
use KitsuneTech\Velox\Structures\Model;
use KitsuneTech\Velox\Database\Procedures\{Query, PreparedStatement, StatementSet, Transaction};
use function KitsuneTech\Velox\Database\oneShot;

/**
 * Simply spits out usage instructions and ends the script.
 *
 * @return void
 */
function printUsage(): void {
    echo "Usage: ./import.php [resume type flag] --hs <hardware/software CSV> --sv <software/vehicle CSV> <vehicle info flag>\n";
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
/* Define initial variables */
$hs = null;
$sv = null;
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
            $hs = $arguments[$i+1];
            $i++;
            break;
        case '--sv':
            $sv = $arguments[$i+1];
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

if (!$hs && !$sv) {
    echo "At least one CSV file must be specified for import.\n\n";
    printUsage();
}
if ($sv && !$info_type){
    echo "Either --use-vin or --use-spec is required if a software/vehicle CSV file is specified.\n\n";
    printUsage();
}
if ($hs && !file_exists($hs)) {
    die("Error: file $hs does not exist.\n\n");
}
if ($sv && !file_exists($sv)) {
    die("Error: file $sv does not exist.\n\n");
}

//$hsPointer and/or $svPointer are implicitly set here
if ($hs && !$hsPointer = fopen($hs, 'r')) {
    die("Error: hardware/software CSV $hs could not be opened. Possibly a permission issue?\n\n");
}
if ($sv && !$svPointer = fopen($sv, 'r')) {
    die("Error: software/vehicle CSV $sv could not be opened. Possibly a permission issue?\n\n");
}

// Parse the CSV file(s) provided
if ($hsPointer){
    while ($line = fgetcsv($hsPointer)){
        $hsContents[] = array_slice($line,0,2);
    }
    array_shift($hsContents);
    fclose($hsPointer);
}
if ($svPointer){
    while ($line = fgetcsv($svPointer)){
        switch ($info_type) {
            case '--use-vin':
                $svContents[] = array_slice($line,0,2);
                break;
            case '--use-spec':
                $svContents[] = array_slice($line,0,8);
                break;
        }
    }
    array_shift($svContents);
    fclose($svPointer);
}

//Initialize the database connection
$conn = new Connection($server,$dbname,$mysql_user,$mysql_password);

//Insert all hardware/software numbers
if ($hs){
    //First dump the entire dataset into a temporary table
    oneShot(new Query($conn, "CREATE TEMPORARY TABLE t_hs (`inventory_no` VARCHAR(255) NOT NULL,`mfr_software_no` VARCHAR(255) NOT NULL)"));

    //INSERT IGNORE here, along with the NOT NULL and CHECK constraints, sanitize the data for rows missing data; this is thrown out in the process of the INSERT
    $hsInsert = new PreparedStatement($conn, "INSERT IGNORE INTO t_hs (inventory_no, mfr_software_no) VALUES(:0,:1)");
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

if (!$sv) {
    die("Complete.\n\n");
}

// ----- Everything from here on pertains only to vehicle/software matches. ----- //

$svTableSQL = "CREATE TABLE IF NOT EXISTS t_sv (`mfr_software_no` VARCHAR(255) NOT NULL, ";
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
        // The CHECK constraint we used in t_hs would be too cumbersome here with eight columns, so we'll sanitize our empty strings a little differently here.
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
    $placeholderArray[$i] = ":".$i;
}
$placeholders = implode(',',$placeholderArray);
$svInsertSQL .= "VALUES ($placeholders)";

if ($verbose) echo "Creating software/vehicle temp table.\n";
oneShot(new Query($conn, $svTableSQL));

if ($verbose) echo "Inserting CSV data into software/vehicle temp table.\n";
$svInsert = new PreparedStatement($conn,$svInsertSQL);
for ($i = 0; $i < count($svContents); $i++){
    for ($i = 0; $i < count($svContents); $i++){
        $rowHasData = false;
        for ($j = 0; $j < count($svContents[$i]); $j++){
            if ($svContents[$i][$j] == '') {
                $rowHasEmptyString = true;
                $svContents[$i][$j] = null;
            }
            else {
                $rowHasData = true;
            }
        }
        if ($info_type == '--use-vin'){
            //Skip any invalid VINs
            if (!$svContents[$i][1] ||(preg_match("/^[A-HJ-NPR-Z\d]{8}[\dX][A-HJ-NPR-Z\d]{2}\d{6}$/i",$svContents[$i][1])) != 1) continue;
        }
        if ($rowHasData) $svInsert->addParameterSet($svContents[$i]);
    }
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

        $specSQL = "WITH vehicles AS (".
                        "SELECT make_name, model_name, IF(model_year IS NOT NULL, model_year, sequence + year_increment) AS model_year, ".
                        "engine_displacement, trim, series FROM vehicle_identities ".
                        "LEFT JOIN l_VDS USING (vds_id) ".
                        "LEFT JOIN l_yearDigits ON vehicle_identities.year_digit = l_yearDigits.digit ".
                        "INNER JOIN l_models ON vehicle_identities.model_id = l_models.model_id ".
                        "INNER JOIN l_makes ON l_models.make_id = l_makes.make_id) ".
                    "UPDATE t_sv ".
                    "INNER JOIN vehicles ON ".
                        "t_sv.make_name = vehicles.make_name ".
                        "AND t_sv.model_name = vehicles.model_name ".
                        "AND t_sv.model_year = vehicles.model_year ".
                        "AND t_sv.engine_displacement = vehicles.engine_displacement ".
                        "AND (t_sv.series = vehicles.series OR (t_sv.series IS NULL AND vehicles.series IS NULL)) ".
                        "AND (t_sv.trim = vehicles.trim OR (t_sv.trim IS NULL AND vehicles.trim IS NULL)) ".
                    "SET t_sv.vehicle_id = vehicles.vehicle_id";
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
    "`expected_year` SMALLINT(5) UNSIGNED, ".
    "`vehicle_series` VARCHAR(100), ".
    "`vehicle_trim` VARCHAR(100), ".
    "`engine_type_name` VARCHAR(255),".
    "`matched` BOOL DEFAULT FALSE, ".
    "`vehicle_id` INT(11) UNSIGNED, ".
    "UNIQUE (vin_pattern)".
    ")";


oneShot(new Query($conn,$createUnmatchedVehicleTable_SQL));

if ($verbose) echo "Adding missing WMI and VDS records.\n";
oneShot(new Query($conn,"INSERT IGNORE INTO l_WMI (wmi_code) SELECT DISTINCT LEFT(vin,3) FROM t_sv"));
oneShot(new Query($conn,"INSERT IGNORE INTO l_VDS (vds_code, wmi_id) SELECT DISTINCT SUBSTRING(vin,4,5), wmi_id FROM t_sv ".
                                "INNER JOIN l_WMI ON LEFT(vin,3) = l_WMI.wmi_code"));

if ($verbose) echo "Populating t_unmatched_vehicles with necessary data from t_sv and VIN component tables.\n";
$populateUnmatchedVehicle_SQL = "INSERT INTO t_unmatched_vehicles (vin_pattern, vds_id, year_digit, expected_year) ".
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
    "SELECT DISTINCT CONCAT(LEFT(vin,8),'_',SUBSTRING(vin,10,1)), software_id ".
    "FROM t_sv ".
    "WHERE software_id IS NOT NULL";

oneShot(new Query($conn,$populateUnmatchedSoftware_SQL));

//If the script dies during the vPIC calls, we can restart the process here with the --resume-vpic flag
resume_vpic:

if (!isset($conn)) $conn = new Connection($server,$dbname,$mysql_user,$mysql_password);

if ($verbose) echo "Batching unmatched VINs from t_unmatched_vehicles.\n";
$unmatchedVins = oneShot(new Query($conn,"SELECT vin_pattern FROM t_unmatched_vehicles WHERE matched = FALSE"));
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
    $updateUnmatched = new StatementSet($conn,"UPDATE t_unmatched_vehicles SET <<values>> WHERE <<condition>>",Query::QUERY_UPDATE);
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
                "where"=> ["vin_pattern"=>["=",$batches[$i][$j]['vin_pattern']]]
            ];
			$valid++;
		}
	}
    $totalValid += $valid;
    if ($verbose) echo "Batch $i returned $valid valid vehicles.\n\n";

    //Update the rows for this batch and clear the array for the next iteration
    $updateUnmatched->addCriteria($updateCriteria);
    $updateUnmatched();
    $updateCriteria = [];
}

if ($verbose) echo "$totalValid valid VIN patterns found in $unmatchedVinCount requests.\n\n";

if ($verbose) echo "*** vPIC server requests completed. Processing results... ***\n\n";

//If the script dies during the vPIC calls and we just want to process what didn't make it, we can skip here with --resume-processing
resume_processing:

if (!isset($conn)) $conn = new Connection($server,$dbname,$mysql_user,$mysql_password);


//--------------------------------------------//
if ($verbose) echo "Retrieving vPIC-matched vehicles from t_unmatched_vehicles.";
$responseArray = oneShot(new Query($conn,"SELECT * FROM t_unmatched_vehicles WHERE matched = TRUE"));
$responseCount = count($responseArray);

$incrementAdjustment = new PreparedStatement($conn,"UPDATE l_VDS SET year_increment = :new_increment WHERE vds_id = :vds");
$vdsAdjustments = [];
for ($i=0; $i<$responseCount; $i++){
	$adjustment = $responseArray[$i]["model_year"] - $responseArray[$i]["expected_year"];
	$vds = $responseArray[$i]["vds_id"];
	if ($adjustment != 0 && !isset($vdsAdjustments[$vds])){
		$vdsAdjustments[$vds] = $adjustment;
	}
}
if ($verbose) echo "Adjusting year increments for ".count($vdsAdjustments)." vds_id's.\n\n";
foreach ($vdsAdjustments as $vds => $adjustment){
	$incrementAdjustment->addParameterSet(["new_increment"=>$adjustment,"vds"=>$vds]);
}
$incrementAdjustment();

//--------------------------------------------//

if ($verbose) echo "Synchronizing l_makes...\n\n";

//vPIC sends makes in all upper-case, so we need to convert them to title case to make them consistent with what we use.
array_walk($responseArray,function(&$elem, $key){ $elem['make_name'] = ucwords(strtolower($elem['make_name'])); });

$makes = array_values(array_unique(array_column($responseArray,"make_name")));

$makesModel = new Model(
	new PreparedStatement($conn, "SELECT make_id, make_name FROM l_makes"),
	null,
	new PreparedStatement($conn, "INSERT INTO l_makes (make_name) VALUES (:make)")
);
$insertMakes = array_values(array_diff($makes, array_column($makesModel->data(),"make_name")));
$insertCount = count($insertMakes);
if ($insertCount > 0){
    if ($verbose) echo "Inserting $insertCount new makes...\n\n";
	$insertRows = [];
	for ($i=0; $i<$insertCount; $i++){
		$insertRows[] = ["make"=>$insertMakes[$i]];
	}
	$makesModel->insert($insertRows);
}
$makesArray = [];
for ($i=0; $i<count($makesModel); $i++){
	$makesArray[$makesModel[$i]["make_name"]] = $makesModel[$i]["make_id"];
}
for ($i=0; $i<$responseCount; $i++){
	$responseArray[$i]["make_id"] = $makesArray[$responseArray[$i]["make_name"]];
}

//--------------------------------------------//

if ($verbose) echo "Synchronizing l_models...\n\n";

$modelsModel = new Model(
	new PreparedStatement($conn, "SELECT model_id, make_id, model_name FROM l_models"),
	null,
	new PreparedStatement($conn, "INSERT INTO l_models (make_id, model_name) VALUES (:make_id, :model_name)")
);
//Here we need to compare unique combinations of make_id + model_name, so we need to make a combination array for each side
$responseModels = [];
$storedModels = [];
for ($i=0; $i<$responseCount; $i++){
	$responseModels[] = json_encode(["make_id" => $responseArray[$i]["make_id"], "model_name" => $responseArray[$i]["model_name"]]);
}
for ($i=0; $i<count($modelsModel); $i++){
	$storedModels[$modelsModel[$i]["model_id"]] = json_encode(["make_id" => $modelsModel[$i]["make_id"], "model_name" => $modelsModel[$i]["model_name"]]);
}

$insertModels = array_values(array_unique(array_diff($responseModels, $storedModels)));
$insertCount = count($insertModels);
if ($insertCount > 0){
    if ($verbose) echo "Inserting $insertCount new models...\n\n";
	array_walk($insertModels,function(&$elem, $key){ $elem = json_decode($elem,true); });
	$modelsModel->insert($insertModels);

	//Refresh $storedModels to get the new model_ids after insert
	$storedModels = [];
	for ($i=0; $i<count($modelsModel); $i++){
		$storedModels[$modelsModel[$i]["model_id"]] = json_encode(["make_id" => $modelsModel[$i]["make_id"], "model_name" => $modelsModel[$i]["model_name"]]);
	}
}
//Attach model_ids to $responseArray depending on make_id and model_name (use the JSON-encoded combination as a key)
$storedModels = array_flip($storedModels);
for ($i=0; $i<$responseCount; $i++){
	$responseArray[$i]["model_id"] = $storedModels[json_encode(["make_id"=> $responseArray[$i]["make_id"], "model_name" => $responseArray[$i]["model_name"]])];
	$responseArray[$i]["UN_vds_year"] = $responseArray[$i]["vds_id"]."-".$responseArray[$i]["year_digit"];
}
//--------------------------------------------//

if ($verbose) echo "Synchronizing l_engine_types...\n";

$types = array_values(array_unique(array_filter(array_column($responseArray,"engine_type_name"))));

$engineTypeModel = new Model(
	new PreparedStatement($conn, "SELECT engine_type_id, engine_type_name FROM l_engine_types"),
	null,
	new PreparedStatement($conn, "INSERT INTO l_engine_types (engine_type_name) VALUES (:engine_type_name)")
);

$insertTypes = array_values(array_unique(array_diff($types, array_column($engineTypeModel->data(),"engine_type_name"))));
$insertCount = count($insertTypes);
if ($insertCount > 0){
        echo "Inserting $insertCount new engine types...\n\n";
        $insertRows = [];
        for ($i=0; $i<$insertCount; $i++){
               	if (!!$insertTypes[$i]) $insertRows[] = ["engine_type_name"=>$insertTypes[$i]];
        }
        $engineTypeModel->insert($insertRows);
}
$typesArray = [];
for ($i=0; $i<count($engineTypeModel); $i++){
        $typesArray[$engineTypeModel[$i]["engine_type_name"]] = $engineTypeModel[$i]["engine_type_id"];
}
for ($i=0; $i<$responseCount; $i++){
       	$responseArray[$i]["engine_type_id"] = $responseArray[$i]["engine_type_name"] ? $typesArray[$responseArray[$i]["engine_type_name"]] : null;
}


//--------------------------------------------//
// Insert into vehicle_identities. vds_id and year_digit are paired as a unique key. model_id is required. vehicle_trim, vehicle_series, engine_displacement, and engine_type_id are optional.
// engine_type_id defaults to 1 (gasoline). 2 is diesel.

// Find what needs to be inserted and what needs to be updated
$identitiesModel = $modelsModel = new Model(
        new PreparedStatement($conn, "SELECT vehicle_id, vds_id, model_id, vehicle_trim, vehicle_series, engine_displacement, year_digit, engine_type_id, CONCAT(vds_id,'-',year_digit) AS UN_vds_year FROM vehicle_identities"),
        new StatementSet($conn, "UPDATE vehicle_identities SET <<values>> WHERE <<condition>>"),
        new StatementSet($conn, "INSERT INTO vehicle_identities <<columns>> <<values>>")
);
$insertArray = [];
$updateArray = [];

//Flip this so we can use the faster isset()
$response_vds_year = array_flip(array_column($identitiesModel->data(),"UN_vds_year"));
for ($i=0; $i<$responseCount; $i++){
	//Clear keys that are no longer needed
	$deleteKeys = ['vin_pattern','make_name','make_id','model_name','expected_year','model_year','engine_type_name'];
	foreach ($deleteKeys as $key){
		unset ($responseArray[$i][$key]);
	}

	if (isset($response_vds_year[$responseArray[$i]["UN_vds_year"]])){
		//Update
		foreach ($responseArray[$i] as $key => $value){
			if (!$value) $responseArray[$i][$key] = null;
		}
		$where = ['vds_id' => ['=',$responseArray[$i]['vds_id']], 'year_digit' => ['=',$responseArray[$i]['year_digit']]];
		unset ($responseArray[$i]['vds_id']);
		unset ($responseArray[$i]['year_digit']);
		unset ($responseArray[$i]['UN_vds_year']);
		$updateArray[] = ["values" => [$responseArray[$i]],"where" => $where];
	}
	else {
		//Insert
		foreach ($responseArray[$i] as $key => $value){
			if (!$value) unset ($responseArray[$i][$key]);
		}
		unset ($responseArray[$i]['UN_vds_year']);
		$insertArray[] = ["values"=>$responseArray[$i]];
	}
}
$updateCount = count($updateArray);
$insertCount = count($insertArray);
if ($verbose) echo "Identities to be updated: $updateCount\n";
if ($verbose) echo "Identities to be inserted: $insertCount\n\n";
if ($updateCount > 0){
    if ($verbose) echo "Updating $updateCount identities...\n";
	$identitiesModel->update($updateArray);
}
if ($insertCount > 0){
    if ($verbose) echo "Inserting $insertCount identities...\n";
	$identitiesModel->insert($insertArray);
}

if ($verbose) echo "Matching new vehicle_ids to t_unmatched_vehicles.\n";
oneShot(new PreparedStatement($conn,
    "WITH currentPatterns AS (SELECT vehicle_id, CONCAT(wmi_code,vds_code,'_',year_digit,'%') AS vin_pattern ".
                "FROM l_WMI ".
                "INNER JOIN l_VDS USING (wmi_id) ".
                "INNER JOIN vehicle_identities USING (vehicle_id) )".
        "UPDATE t_unmatched_vehicles ".
        "INNER JOIN currentPatterns USING (vin_pattern) ".
        "SET t_unmatched_vehicles.vehicle_id = currentPatterns.vehicle_id"));

if ($verbose) echo "Adding new records in vehicle_software_map for vPIC-matched vehicles and software.\n";
oneShot(new Query($conn,
    "INSERT INTO vehicle_software_map (vehicle_id, software_id) ".
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