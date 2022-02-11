<?php

namespace NDAPio\MongoHelper;

class MongoModel {

    public $id;
    public $b64id;
    public $created_time;
    public $updated_time;

    public function __construct() {

    }

    public function map($document) {
        if (isset($document["id"])) {
            $this->id = $document["id"];
            $this->b64id = base64_encode($document["id"]);
        } else {
            $this->id = "";
            $this->b64id = "";
        }
        if (isset($document["created_time"])) {
            $this->created_time = $document["created_time"];
        } else {
            $this->created_time = "";
        }
        if (isset($document["updated_time"])) {
            $this->updated_time = $document["updated_time"];
        } else {
            $this->updated_time = "";
        }
    }

    public function toArray() {
        return json_decode(json_encode($this), true);
    }

}
