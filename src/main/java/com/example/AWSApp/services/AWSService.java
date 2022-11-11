package com.example.AWSApp.services;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AWSService {

    @Value("${application.bucket.name}")
    private String bucketName;

    @Autowired
    private AmazonS3 s3Client;


    public String uploadFile(MultipartFile file) {
        File fileObj = convertMultiPartFileToFile(file);
        String fileName = file.getOriginalFilename();
        s3Client.putObject(new PutObjectRequest(bucketName, fileName, fileObj));
        fileObj.delete();
        return "File uploaded : " + fileName;
    }


    public byte[] downloadFile(String fileName) {
        S3Object s3Object = s3Client.getObject(bucketName, fileName);
        S3ObjectInputStream inputStream = s3Object.getObjectContent();
        try {
            byte[] content = IOUtils.toByteArray(inputStream);
            return content;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public String deleteFile(String fileName) {
        s3Client.deleteObject(bucketName, fileName);
        return fileName + " removed ...";
    }

    public List<String> listAllFiles() {
        ListObjectsV2Result listObjectsV2Result = s3Client.listObjectsV2(bucketName);
        return listObjectsV2Result.getObjectSummaries()
                .stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());
    }

    public String readFile(String fileName){
        S3Object objS3 = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
        InputStream objectData = objS3.getObjectContent();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(objS3.getObjectContent()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
//                return line;
            }
            objectData.close();
        }catch (IOException e){
            e.printStackTrace();

        }
        return null;
    }


    private File convertMultiPartFileToFile(MultipartFile file) {
        File convertedFile = new File(file.getOriginalFilename());
        try (FileOutputStream fos = new FileOutputStream(convertedFile)) {
            fos.write(file.getBytes());
        } catch (IOException e) {
            log.error("Error converting multipartFile to file", e);
        }
        return convertedFile;
    }

    // @Async annotation ensures that the method is executed in a different background thread
    // but not consume the main thread.
//    @Async
//    public void uploadFile(final MultipartFile multipartFile) {
////        LOGGER.info("File upload in progress.");
//        try {
//            final File file = convertMultiPartFileToFile(multipartFile);
//            uploadFileToS3Bucket(bucketName, file);
////            LOGGER.info("File upload is completed.");
//            file.delete();    // To remove the file locally created in the project folder.
//        } catch (final AmazonServiceException ex) {
////            LOGGER.info("File upload is failed.");
////            LOGGER.error("Error= {} while uploading file.", ex.getMessage());
//        }
//    }

//    private File convertMultiPartFileToFile(final MultipartFile multipartFile) {
//        final File file = new File(multipartFile.getOriginalFilename());
//        try (final FileOutputStream outputStream = new FileOutputStream(file)) {
//            outputStream.write(multipartFile.getBytes());
//        } catch (final IOException ex) {
////            LOGGER.error("Error converting the multi-part file to file= ", ex.getMessage());
//        }
//        return file;
//    }

//    private void uploadFileToS3Bucket(final String bucketName, final File file) {
//        final String uniqueFileName = file.getName();
////        LocalDateTime.now() + "_" +
////        LOGGER.info("Uploading file with name= " + uniqueFileName);
//        final PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, uniqueFileName, file);
//        s3Client.putObject(putObjectRequest);
//    }
}