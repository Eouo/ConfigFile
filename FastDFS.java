import org.apache.commons.io.IOUtils;
import org.csource.common.MyException;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;
import org.junit.Test;

import java.io.*;
import java.util.Properties;

public class FastDFS {

    /*fastDFS 文件上传测试*/
    @Test
    public void fastDFSUploadTest() {
        try {
        /*配置tracker(DFS 的协调者)*/
            Properties properties = new Properties();
            /*两个Tracker  (集群) */
            properties.setProperty(ClientGlobal.PROP_KEY_TRACKER_SERVERS, "192.168.101.128:22122,192.168.101.129:22122");
            /*初始化协调者*/
            ClientGlobal.initByProperties(properties);


        /*文件上传*/
        /*客户端的协调者*/
            TrackerClient trackerClient = new TrackerClient();
            /*协调者服务器*/
            TrackerServer trackerServer = trackerClient.getConnection();
            /*存储服务器*/
            StorageServer storageServer = null;
            /*存储的客户端*/
            StorageClient storageClient = new StorageClient(trackerServer, storageServer);

            /*Metadata   给上传的文件添加一个标签*/
            NameValuePair[] nameValuePairs = new NameValuePair[3];
            nameValuePairs[0] = new NameValuePair("Name:", "qi");
            nameValuePairs[1] = new NameValuePair("time:", "0902");
            nameValuePairs[2] = new NameValuePair("height", "200px");



            /*获得一个文件输入流*/
            InputStream inputStream = new FileInputStream(new File("D:/Picture/221197.jpg"));

            String[] arr = storageClient.upload_file(IOUtils.toByteArray(inputStream), "jpg", nameValuePairs);

            for (String n : arr) {
                System.out.println(n);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (MyException e) {
            e.printStackTrace();
        }
    }

    /*文件下载*/
    @Test
    public void fastDFSDownloadTest() throws IOException, MyException {


          /*配置tracker(DFS 的协调者)*/
        Properties properties = new Properties();
        properties.setProperty(ClientGlobal.PROP_KEY_TRACKER_SERVERS, "192.168.101.128:22122");
            /*初始化协调者*/
        ClientGlobal.initByProperties(properties);

        TrackerClient trackerClient = new TrackerClient();
        TrackerServer trackerServer = trackerClient.getConnection();
        StorageServer storageServer = null;

        StorageClient storageClient = new StorageClient(trackerServer, storageServer);
        byte[] bytes = storageClient.download_file("group1", "M00/00/00/wKhlg1oOqbqAdpDwAANZkHW1QeM602.jpg");

        /*获得文件信息*/
        FileInfo fileInfo = storageClient.get_file_info("group1", "M00/00/00/wKhlg1oOqbqAdpDwAANZkHW1QeM602.jpg");
        System.out.println("获得用户端IP地址:" + fileInfo.getSourceIpAddr());
        System.out.println("检错码:" + fileInfo.getCrc32());
        System.out.println("上传文件的时间戳:" + fileInfo.getCreateTimestamp());
        System.out.println("获得文件的大小:" + fileInfo.getFileSize());
        System.out.println("装化为字符串:" + fileInfo.toString());


        /*获得data信息*/
        NameValuePair[] nameValuePairs = storageClient.get_metadata("group1", "M00/00/00/wKhlg1oOqbqAdpDwAANZkHW1QeM602.jpg");
        for (NameValuePair n : nameValuePairs) {
            System.out.println(n.getName() + "  >>>>>>>" + n.getValue());
        }



        /*获得输出流*/
        OutputStream outputStream = new FileOutputStream(new File("D:/Desktop/wKhlg1oOqbqAdpDwAANZkHW1QeM602.jpg"));

        IOUtils.write(bytes, outputStream);


    }

}
