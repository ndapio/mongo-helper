<?php
namespace NDAPio\MongoHelper;

use MongoDB;
use MongoDB\Driver\Command;
use MongoDB\Driver\Manager;

class MongoConnector {

    protected $domain;
    protected $port;
    protected $username;
    protected $password;
    protected $database;
    protected $mongodb;

    public function __construct($domain, $port, $username, $password, $database) {
        $this->port = $port;
        $this->domain = $domain;
        $this->username = $username;
        $this->password = $password;
        $this->database = $database;
    }

    public function switchDB($database) {
        $this->database = $database;
    }

    public function handleException($e) {

    }

    public function connect() {
        if ($this->domain != "" && $this->port != "") {
            if ((intval($this->port) > 0) && (intval($this->port) <= 65535)) {
                try {
                    $this->mongodb = new MongoDB\Driver\Manager("mongodb://" . $this->domain . ":" . $this->port, array("username" => $this->username, "password" => $this->password));
                    $command = new MongoDB\Driver\Command(['ping' => 1]);
                    $cursor = $this->mongodb->executeCommand($this->database, $command);
                    $result = $cursor->toArray();
                    if (is_array($result)) {
                        return true;
                    }
                } catch (Exception $e) {
                    $this->handleException($e);
                }
            }

        }
        return false;
    }

    public function query($collection, $args = array()) {
        if (isset($args["filters"]) && is_array($args["filters"])) {
            $filters = $args["filters"];
        } else {
            $filters = array();
        }
        if (isset($args["limit"])) {
            $limit = intval($args["limit"]);
        } else {
            $limit = 10000000;
        }
        if (isset($args["offset"])) {
            $offset = intval($args["offset"]);
        } else {
            $offset = 0;
        }
        if (isset($args["sortBy"])) {
            $sort_field = $args["sortBy"];
        } else {
            $sort_field = "";
        }
        if (isset($args["sort"])) {
            $sort_order = $args["sort"];
        } else {
            $sort_order = "asc";
        }
        return $this->queryDB($collection, $filters, $limit, $offset, $sort_field, $sort_order);
    }

    public function queryDB($collection, $filters = [], $limit = 100, $offset = 0, $sort_field = "", $sort_order = "asc") {
        if ($this->connect()) {
            if (count($filters) > 0) {
                $old_filters = $filters;
                $filters = array();
                foreach ($old_filters as $key => $value) {
                    if ($key == "_id") {
                        if (strlen($value) > 24) {
                            return array(
                                "status" => "error",
                                "message" => "invalid document id"
                            );
                        } else {
                            $value = new MongoDB\BSON\ObjectId($value);
                        }
                    }
                    $filters[$key] = $value;
                }
                if ($sort_field == "") {
                    $command = new MongoDB\Driver\Command(['find' => $collection, 'filter' => $filters, 'limit' => $limit, 'skip' => $offset]);
                } else {
                    if ($sort_order == "asc") {
                        $order = 1;
                    } else {
                        $order = -1;
                    }
                    $command = new MongoDB\Driver\Command(['find' => $collection, 'filter' => $filters, 'limit' => $limit, 'skip' => $offset, 'sort' => [$sort_field=>$order]]);
                }
            } else {
                if ($sort_field == "") {
                    $command = new MongoDB\Driver\Command(['find' => $collection, 'limit' => $limit, 'skip' => $offset]);
                } else {
                    if ($sort_order == "asc") {
                        $order = 1;
                    } else {
                        $order = -1;
                    }
                    $command = new MongoDB\Driver\Command(['find' => $collection, 'limit' => $limit, 'skip' => $offset, 'sort' => [$sort_field=>$order]]);
                }
            }
            try {
                $count_command = new MongoDB\Driver\Command(['count' => $collection, "query" => $filters]);
                $count_cursor = $this->mongodb->executeCommand($this->database, $count_command);
                $count_cursor_array = $count_cursor->toArray();
                $cursor = $this->mongodb->executeCommand($this->database, $command);
                $records = $cursor->toArray();
                return array(
                    "status" => "success",
                    "total" => $count_cursor_array[0]->n,
                    "count" => count($records),
                    "records"  => $records
                );
            } catch(MongoDB\Driver\Exception $e) {
                return array(
                    "status" => "error",
                    "message" => $e->getMessage()
                );
            }
        } else {
            return array(
                "status" => "error",
                "message" => "can not connect to database"
            );
        }
    }

    public function insert($collection, $dataset) {
        if ($this->connect()) {
            if (count($dataset) == 0) {
                return array(
                    "status" => "error",
                    "message" => "no documents"
                );
            }
            try {
                $command = new MongoDB\Driver\Command(['insert' => $collection, 'documents' => $dataset]);
                $cursor = $this->mongodb->executeCommand($this->database, $command);
                $data = $cursor->toArray()[0];
                return array(
                    "status" => "success",
                    "count" => $data->n
                );
            } catch(MongoDB\Driver\Exception $e) {
                return array(
                    "status" => "error",
                    "message" => $e->getMessage()
                );
            }
        } else {
            return array(
                "status" => "error",
                "message" => "can not connect to database"
            );
        }
    }

    public function insertIfNotExists($collection, $dataset, $data_key, $data_value) {
        if ($this->connect()) {
            if (count($dataset) == 0) {
                return array(
                    "status" => "error",
                    "message" => "no documents"
                );
            }
            try {
                $filters = array(
                    $data_key => $data_value
                );
                $command = new MongoDB\Driver\Command(['count' => $collection, 'query' => $filters]);
                $cursor = $this->mongodb->executeCommand($this->database, $command);
                $data = $cursor->toArray()[0];
                if (isset($data->n) && (int)$data->n == 0) {
                    $command = new MongoDB\Driver\Command(['insert' => $collection, 'documents' => $dataset]);
                    $cursor = $this->mongodb->executeCommand($this->database, $command);
                    $data = $cursor->toArray()[0];
                    return array(
                        "status" => "success",
                        "count" => $data->n
                    );
                }
            } catch(MongoDB\Driver\Exception $e) {
                return array(
                    "status" => "error",
                    "message" => $e->getMessage()
                );
            }
        } else {
            return array(
                "status" => "error",
                "message" => "can not connect to database"
            );
        }
    }

    public function bulkWrite($action, $collection, $bulk_args, $multi = true, $upsert = false) {
        if ($this->connect()) {
            try {
                $bulk = new MongoDB\Driver\BulkWrite(['ordered' => true]);
                foreach ($bulk_args as $bulk_arg) {
                    if (isset($bulk_arg["key"]["_id"]) && $bulk_arg["key"]["_id"] != "") {
                        if (strlen($bulk_arg["key"]["_id"]) > 24) {
                            return array(
                                "status" => "error",
                                "message" => "invalid document id"
                            );
                        } else {
                            $bulk_arg["key"]["_id"] = new MongoDB\BSON\ObjectId($bulk_arg["key"]["_id"]);
                        }
                    }
                    if ($action == "insert") {
                        $bulk->insert($bulk_arg["documents"]);
                    }
                    if ($action == "update") {
                        $bulk->update($bulk_arg["key"], ['$set' => $bulk_arg["documents"]], ['multi' => $multi, 'upsert' => $upsert]);
                    }
                    if ($action == "delete") {
                        $bulk->delete($bulk_arg["key"]);
                    }
                }
                $writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 100);
                $result = $this->mongodb->executeBulkWrite($this->database.".".$collection, $bulk, $writeConcern);
                return array(
                    "status" => "success",
                    "result" => $result->getModifiedCount()
                );
            }
            catch(MongoDB\Driver\Exception $e) {
                return array(
                    "status" => "error",
                    "message" => $e->getMessage()
                );
            }
        } else {
            return array(
                "status" => "error",
                "message" => "can not connect to database"
            );
        }
    }

    public function empty($collection = "") {
        if ($this->connect()) {
            $database = $this->database;
            if ($collection != "") {
                // empty collection
                $command = new MongoDB\Driver\Command(['find' => $collection]);
                $entity = $this->mongodb->executeCommand($database, $command)->toArray();
                if (count($entity) > 0) {
                    $command = new MongoDB\Driver\Command(['drop' => $collection]);
                    $this->mongodb->executeCommand($database, $command);
                }
            } else {
                // empty database
                $command = new MongoDB\Driver\Command(['find' => $database]);
                $entity = $this->mongodb->executeCommand($database, $command)->toArray();
                if (count($entity) > 0) {
                    $command = new MongoDB\Driver\Command(['drop' => $database]);
                    $this->mongodb->executeCommand($database, $command);
                }
            }
        }
    }
}