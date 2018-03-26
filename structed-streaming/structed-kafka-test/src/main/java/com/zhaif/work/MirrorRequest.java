package com.zhaif.work;

import java.io.Serializable;
import java.sql.Timestamp;

public class MirrorRequest implements Serializable {
    /**
     * 
     */
    public static String Url2Path(String url) {
        if (url.startsWith("/article") || url.startsWith("/paper")) {
            return "article";
        }
        return "other";
    }

    public MirrorRequest(String host, String url) {
        this.host = host;
        this.path = MirrorRequest.Url2Path(url);
        this.timeStamp = new Timestamp(System.currentTimeMillis());
    }

    public MirrorRequest(String host, String url, long time) {
        this.host = host;
        this.path = MirrorRequest.Url2Path(url);
        this.timeStamp = new Timestamp(time);
    }

    private static final long serialVersionUID = 1L;
    private String host;
    private String path;
    private Timestamp timeStamp;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Timestamp getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Timestamp timeStamp) {
        this.timeStamp = timeStamp;
    }
}
