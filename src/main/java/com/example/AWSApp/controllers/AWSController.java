package com.example.AWSApp.controllers;

import com.example.AWSApp.services.AWSService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/file")
public class AWSController {

    @Autowired
    private AWSService service;

    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(@RequestPart(value = "file") MultipartFile file) {
        service.uploadFile(file);
        final String response = "[" + file.getOriginalFilename() + "] uploaded successfully.";
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/download/{fileName}")
    public ResponseEntity<ByteArrayResource> downloadFile(@PathVariable String fileName) {
        byte[] data = service.downloadFile(fileName);
        ByteArrayResource resource = new ByteArrayResource(data);
        return ResponseEntity
                .ok()
                .contentLength(data.length)
                .header("Content-type", "application/octet-stream")
                .header("Content-disposition", "attachment; filename=\"" + fileName + "\"")
                .body(resource);
    }

    @DeleteMapping("/delete/{fileName}")
    public ResponseEntity<String> deleteFile(@PathVariable String fileName) {
        return new ResponseEntity<>(service.deleteFile(fileName), HttpStatus.OK);
    }

    @GetMapping("/getallfiles")
    public List<String> getAllFiles (){
        return service.listAllFiles();
    }

    @GetMapping("/readfile/{fileName}")
    public ResponseEntity<String> read(@PathVariable String fileName){
        return new ResponseEntity<>(service.readFile(fileName), HttpStatus.OK);
    }
}