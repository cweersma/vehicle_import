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
use function KitsuneTech\Velox\Transport\Export;

/**
 * Simply spits out usage instructions and ends the script.
 *
 * @return void
 */
function printUsage(): void {
    echo "Usage: ./import.php --hs <hardware/software CSV> --sv <software/vehicle CSV> <vehicle info flag>\n";
    echo "\n";
    echo "Vehicle info flags:\n";
    echo "    --use-vin     The software/vehicle CSV contains VINs.\n";
    echo "    --use-spec    The software/vehicle CSV contains vehicle specifications.\n";
    echo "\n";
    die ("See README.md for more detailed information.\n\n");
}
/* Define initial variables */
$hs = null;
$sv = null;
$info_type = null;
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
                echo "import.php requires exactly one of --use-vin or --use-spec.\n\n";
                printUsage();
            }
            break;
        case '--verbose':
            $verbose = true;
            break;
    }
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
    fclose($svPointer);
}

//Initialize the database connection
$conn = new Connection($server,$dbname,$mysql_user,$mysql_password);

//Insert all hardware/software numbers
if ($hs){
    //First dump the entire dataset into a temporary table
    oneShot(new Query($conn, "CREATE TEMPORARY TABLE t_hs (`inventory_no` VARCHAR(255) NOT NULL,`mfr_software_no` VARCHAR(255) NOT NULL) CONSTRAINT noEmpty CHECK (inventory_no <> '' AND mfr_software_no <> '')"));

    //INSERT IGNORE here, along with the NOT NULL and CHECK constraints, sanitize the data for rows missing data; this is thrown out in the process of the INSERT
    $hsInsert = new PreparedStatement("INSERT IGNORE INTO t_hs (inventory_no, mfr_software_no) VALUES(?,?)");
    for ($i = 0; $i < count($hsContents); $i++){
        $hsInsert->addParameterSet($hsContents[$i]);
    }
    $hsInsert();

    //Perform both inserts as an atomic transaction using the temporary table as a source
    $hsTransaction = new Transaction($conn);
    $hsQueries = [
        new Query($conn,"INSERT IGNORE INTO inventory (inventory_no) SELECT DISTINCT inventory_no FROM t_hs"),
        new Query($conn, "INSERT IGNORE INTO software (inventory_id, mfr_software_no) SELECT inventory_id, mfr_software_no FROM t_hs INNER JOIN inventory USING (inventory_id)")
    ];
    foreach ($hsQueries as $query){
        $hsTransaction->addQuery($query);
    }
    $hsTransaction();
    oneShot(new Query($conn,"DROP TABLE t_hs"));
}

if (!$sv) {
    die("Complete.\n\n");
}

// ----- Everything from here on pertains only to vehicle/software matches. ----- //

$svTableSQL = "CREATE TEMPORARY TABLE t_sv (`mfr_software_no` VARCHAR(255) NOT NULL, ";
$svInsertSQL = "INSERT IGNORE INTO t_sv (mfr_software_no, )";
switch ($info_type) {
    case '--use-vin':
        $svTableSQL .= "`vin` VARCHAR(17) NOT NULL)";
        $svInsertSQL .= "vin, ";
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
$placeholders = implode(",",str_split(str_repeat('?',$colCount)));
$svInsertSQL .= "VALUES ($placeholders)";
oneShot(new Query($conn, $svTableSQL));
$svInsert = new PreparedStatement($conn,$svInsertSQL);
for ($i = 0; $i < count($svContents); $i++){
    //Replace all empty strings with nulls here; this will allow the NOT NULL constraints we set to work properly
    $row = array_map(function($value){
        return $value === "" ? null : $value;
    },$svContents[$i]);
    $svInsert->addParameterSet($row);
}
$svInsert();

//Update t_sv with software_ids
oneShot(new Query($conn,"UPDATE t_sv INNER JOIN software USING (mfr_software_no) SET t_sv.software_id = software.software_id"));

//t_sv vehicle_id matching will depend on whether we're working with VINs or vehicle specs.
switch ($info_type){
    case "--use-vin":
        //Wildcard joins are rather slow in MariaDB. Maybe if we use the Velox equivalent...? At very least the optimizer won't have to cope with the CTE.
        $vinPatterns = new Model(new PreparedStatement($conn,
            "SELECT vehicle_id, CONCAT(wmi_code,vds_code,'_',year_digit,'%') AS vin_pattern ".
            "FROM l_WMI ".
            "INNER JOIN l_VDS USING (wmi_id) ".
            "INNER JOIN vehicle_identities USING (vehicle_id) "));
        $svData = new Model(new PreparedStatement($conn,"SELECT vin FROM t_sv"));
        $match = $svData->join(INNER_JOIN,$vinPatterns,["vin","LIKE","vin_pattern"]);
        $svUpdate = new PreparedStatement($conn,"UPDATE t_sv SET vehicle_id = :vehicle_id WHERE vin = :vin");
        for ($i = 0; $i < count($match); $i++){
            $svUpdate->addParameterSet(["vin"=>$match[$i]["vin"],"vehicle_id"=>$match[$i]["vehicle_id"]]);
        }
        $svUpdate();
        break;
    case "--use-spec":
        //TODO: Add vehicle matching based on specifications
        break;
}

$resultModel = new Model(new PreparedStatement($conn,"SELECT * FROM nomatch_vpic"));

//$rows = count($resultModel);
$startIndex = 1000;
$rows = 1000;
$batches = [];
$currentBatch = [];

//Build batches of 50
for ($i=$startIndex; $i<$rows+$startIndex; $i++){
	$currentBatch[] = $resultModel[$i];
	if (count($currentBatch) == 50 || $i == $rows - 1){
		$batches[] = $currentBatch;
		$currentBatch = [];
	}
}

$responseArray = [];
$batchCount = count($batches);
for ($i=0; $i<$batchCount; $i++){
	//Concatenate to string
	$batchStr = implode(";",array_column($batches[$i],"vpicQuery"));

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
	for ($j=0; $j<count($batches[$i]); $j++){
		$vinResponse = $responseObj->Results[$j];
		if ($vinResponse->Model){
			$responseArray[] = [
				"vin_pattern"=>$batches[$i][$j]['vpicQuery'],
				"vds_id"=>$batches[$i][$j]['vds_id'],
				"year_digit"=>$batches[$i][$j]['year_digit'],
				"engine_displacement"=>$vinResponse->DisplacementL,
				"make_name"=>$vinResponse->Make,
				"model_name"=>$vinResponse->Model,
				"expected_year"=>$batches[$i][$j]['expected_year'],
				"model_year"=>$vinResponse->ModelYear,
				"vehicle_series"=>$vinResponse->Series,
				"vehicle_trim"=>$vinResponse->Trim,
				"engine_type_name"=>$vinResponse->FuelTypePrimary
			];
			$valid++;
		}
	}
	echo "Batch $i returned $valid valid vehicles.\n\n";
}
$responseCount = count($responseArray);
echo "$responseCount valid VIN patterns found in $rows requests.\n\n";

echo "*** vPIC server requests completed. Processing results... ***\n\n";

//--------------------------------------------//

$incrementAdjustment = new PreparedStatement($conn,"UPDATE l_VDS SET year_increment = :new_increment WHERE vds_id = :vds");
$vdsAdjustments = [];
for ($i=0; $i<$responseCount; $i++){
	$adjustment = $responseArray[$i]["model_year"] - $responseArray[$i]["expected_year"];
	$vds = $responseArray[$i]["vds_id"];
	if ($adjustment != 0 && !isset($vdsAdjustments[$vds])){
		$vdsAdjustments[$vds] = $adjustment;
	}
}
echo "Adjusting year increments for ".count($vdsAdjustments)." vds_id's.\n\n";
foreach ($vdsAdjustments as $vds => $adjustment){
	$incrementAdjustment->addParameterSet(["new_increment"=>$adjustment,"vds"=>$vds]);
}
//$incrementAdjustment->execute();

//--------------------------------------------//

echo "Synchronizing l_makes...\n\n";

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
	echo "Inserting $insertCount new makes...\n\n";
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

echo "Synchronizing l_models...\n\n";

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
	echo "Inserting $insertCount new models...\n\n";
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

echo "Synchronizing l_engine_types...\n";

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
echo "Identities to be updated: $updateCount\n";
echo "Identities to be inserted: $insertCount\n\n";
if ($updateCount > 0){
	echo "Updating $updateCount identities...\n";
	$identitiesModel->update($updateArray);
}
if ($insertCount > 0){
	echo "Inserting $insertCount identities...\n";
	$identitiesModel->insert($insertArray);
}
