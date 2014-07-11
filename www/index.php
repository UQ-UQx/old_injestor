<?php
    header('Content-Type: application/json');
    $json = json_encode(presentdir("."));
    echo $json;
?>
<?php


    function presentdir($path) {
        $data = array();
        $d = dir($path);
        while (false !== ($entry = $d->read())) {
            if(substr($entry,0,1) != '.' && strpos($entry,'.php') == false) {
                $fullpath = $path.'/'.$entry;
                $ext = pathinfo($fullpath, PATHINFO_EXTENSION);
                $filename = str_replace('.'.$ext,"",$entry);
                if(is_dir($fullpath)) {
                    $data[$entry] = presentdir($fullpath);
                } else {
                    $addpath = substr($fullpath,2);
                    $fullurl = 'http' . (isset($_SERVER['HTTPS']) ? 's' : '') . '://' . "{$_SERVER['HTTP_HOST']}{$_SERVER['REQUEST_URI']}{$addpath}";
                    $data[$filename] = $fullurl;
                }
            }
        }
        $d->close();
        return $data;
    }
?>