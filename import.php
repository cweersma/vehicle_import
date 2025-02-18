<?php
global $server, $dbname, $mysql_user, $mysql_password;
$url = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/";

include_once "vendor/autoload.php";
include_once "inc/connection.php";

use KitsuneTech\Velox\Database\Connection;
use KitsuneTech\Velox\Structures\Model;
use KitsuneTech\Velox\Database\Procedures\{PreparedStatement, StatementSet};
use function KitsuneTech\Velox\Database\oneShot;
use function KitsuneTech\Velox\Transport\Export;

$conn = new Connection($server,$dbname,$mysql_user,$mysql_password);
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

echo "Synchromizing l_models...\n\n";

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
