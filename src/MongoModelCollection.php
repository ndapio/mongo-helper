<?php

namespace NDAPio\MongoHelper;

class MongoModelCollection {

    protected $list;
    protected $documents;
    protected $mongodb;
    protected $collection;
    protected $class;

    public function __construct($mongodb, $collection, $class = "") {
        $this->mongodb = $mongodb;
        $this->collection = $collection;
        $this->class = $class;
        $this->list = array();
        $this->documents = array();
    }

    public function push($object) {
        array_push($this->list, $object);
        return $this->list;
    }

    public function pop() {
        return array_pop($this->list);
    }

    public function unshift($object) {
        array_unshift($this->list, $object);
        return $this->list;
    }

    public function shift() {
        return array_shift($this->list);
    }

    public function randomize() {
        if (is_array($this->list)) {
            $count = count($this->list);
            if ($count > 0) {
                $pos = mt_rand(0,$count-1);
                if (isset($this->list[$pos])) {
                    return $this->list[$pos];
                }
            }
        }
        return null;
    }

    public function getDocuments() {
        return $this->documents;
    }

    public function getList() {
        return $this->list;
    }

    public function getTotal() {
        if (is_array($this->list)) {
            return count($this->list);
        } else {
            return 0;
        }
    }

    public function toArray() {
        $array = array();
        while($object = $this->pop()) {
            $array[] = $object->toArray();
        }
        return $array;
    }

    public function toArrayKey($value_key, $display_key) {
        $array = array();
        while($object = $this->pop()) {
            if (isset($object->$value_key)) {
                if (isset($object->$display_key)) {
                    $array[$object->$value_key] = $object->$display_key;
                } else {
                    $array[$object->$value_key] = $object->$value_key;
                }
            }
        }
        return $array;
    }

    public function loadDocumentsByID($document_id) {
        $args = array(
            "filters" => array(
                "_id" => $document_id
            )
        );
        self::loadDocuments($args);
    }

    public function loadDocumentsByB64ID($b64id) {
        $args = array(
            "filters" => array(
                "_id" => base64_decode($b64id)
            )
        );
        self::loadDocuments($args);
    }

    public function loadDocuments($query_args) {
        $records = $this->mongodb->query($this->collection, $query_args);
        $documents = array();
        if ($records["status"] == "success" && $records["count"] > 0) {
            foreach ($records["records"] as $record) {
                $new_record = array();
                foreach ($record as $key => $value) {
                    if ($key != "_id" && $key != "id") {
                        $new_record[$key] = $value;
                    } else {
                        $objectId = new \MongoDB\BSON\ObjectId($record->_id);
                        $new_record["id"] = $objectId->jsonSerialize()['$oid'];
                    }
                }
                if (count($new_record) > 0) {
                    if ($this->class != "") {
                        $object = new $this->class();
                        $object->map($new_record);
                        $this->push($object);
                    } else {
                        $this->push($new_record);
                    }
                }
                $documents[] = $new_record;
            }
        }
        $this->documents = $documents;
    }

    public function insertDocuments() {
        $dataset = array();
        $timestamp = time();
        while($object = $this->shift()) {
            $object->created_time = $timestamp;
            $object->updated_time = $timestamp;
            unset($object->id);
            unset($object->b64id);
            $dataset[] = $object;
        }
        return $this->mongodb->insert($this->collection, $dataset);
    }

    public function insertDocumentsIfNotExist($query_key) {
        $results = array();
        $timestamp = time();
        while($object = $this->pop()) {
            if (isset($object->$query_key)) {
                $query_value = $object->$query_key;
            } else {
                $query_value = "";
            }
            $object->created_time = $timestamp;
            $object->updated_time = $timestamp;
            unset($object->id);
            unset($object->b64id);
            $dataset = array($object);
            $results[] = $this->mongodb->insertIfNotExists($this->collection, $dataset, $query_key, $query_value);
        }
        return $results;
    }

    public function insertDocumentsByQuery($query_args) {
        $records = $this->mongodb->query($this->collection, $query_args);
        $results = array();
        if (isset($records["count"])) {
            if ($records["count"] == 0) {
                $dataset = array();
                $timestamp = time();
                while($object = $this->shift()) {
                    $object->created_time = $timestamp;
                    $object->updated_time = $timestamp;
                    unset($object->id);
                    unset($object->b64id);
                    $dataset[] = $object;
                }
                $results = $this->mongodb->insert($this->collection, $dataset);
            }
        }
        return $results;
    }

    public function bulkwriteDocumentsByKey($action, $query_key, $upsert = false) {
        $results = array();
        $index = 0;
        $dataset = array();
        $timestamp = time();
        while($object = $this->pop()) {
            if ($query_key != "_id") {
                $query_value = $object->$query_key ?? "";
            } else {
                $query_value = $object->id ?? "";
            }
            $object->updated_time = $timestamp;
            $dataset[$index]["key"][$query_key] = $query_value;
            $dataset[$index]["documents"] = $object;
            $index++;
        }
        if ($action == "update" || $action == "delete") {
            $results = $this->mongodb->bulkWrite($action, $this->collection, $dataset, true, $upsert);
        }
        return $results;
    }

    public function updateDocumentsByKey($query_key, $upsert = false) {
        return $this->bulkwriteDocumentsByKey("update", $query_key, $upsert);
    }

    public function removeDocumentsByKey($query_key) {
        return $this->bulkwriteDocumentsByKey("delete", $query_key);
    }
}
