## REST Helper

A library that supports communication with REST API. This library can speed up your development with REST API and make it easier to troubleshoot.

 - Built-in code base to interact with Guzzle to call REST API.
 - Built-in code to support Guzzle request under HTTP/HTTPS proxy.
 - Built-in code to get responded cookies.
 - Built-in code for common authentication method: username & password or access token
 - Built-in code to support form params, JSON data, XML data

## Guide and sample

You can extend the RestApi class to use in your project. 

Example: 

    <?php
    use NDAPio\RestHelper\RestApi;
    class SampleRestful extends RestApi {
        public function __construct($base_url = "https://sample.com") {
            $this->base_url = $base_url;
        }
        public function sample() {
            $args = array(
                "path" => "/path",
                "params" => array(
                    "param_1" => "value_1",
                ),
                "headers" => array(
                    "Header 1" => "Value 1"
                ),
                "data_type" => "json:json", // format: input:output of REST API
				// default if not defined: json:json
				// json:json => REST API endpoint gets json and returns json
				// form:json => REST API endpont gets form params (like HTTP Form post) and returns json
				// json:xml => REST API endpoint gets json and returns xml
				// form:xml => REST API endpont gets form params (like HTTP Form post) and returns xml
            );
            return $this->doGET($args); // doPOST, doPUT, doDELETE
	    }
    }
    ?>

> **Note:** There are more samples in **sample** folder

## Response

Success response:

    array(  
      "status" => "success",  
      "time" => $timestamp,  
      "code" => $code,  
      "cookies" => $cookies,  
      "response" => $response_data_parsed // depend on the data_type input  
    );

Error response:

    $response = array(  
      "status" => "error",  
      "time" => $timestamp, 
      "code" => $code // 401, 403, 500, ... 
    );

Exception response:

    array(  
      "status" => "exception",  
      "code" => 000,  
      "time" => $timestamp,  
      "response" => array(  
	      "message" => $exception_message,  
	      "action" => $action,  
	      "headers" => $headers,  
	      "params" => $params,  
	      "proxy" => $proxy  
      )  
    );
