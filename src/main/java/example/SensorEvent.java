package example;

public class SensorEvent {
    private int sensorId;
    private String sensorType;
    private String location;
    private double lat;
    private double lon;
    private long timestamp;
    private double temperature;
    private double humidity;

    public SensorEvent(int sensorId, String sensorType, String location, double lat, double lon, long timestamp,
                       double temperature, double humidity) {
        this.sensorId = sensorId;
        this.sensorType = sensorType;
        this.location = location;
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;
        this.temperature = temperature;
        this.humidity = humidity;
        //System.out.println("SensorEvent created: " + this.getSensorId());  // 添加调试信息
    }

    public int getSensorId() { return sensorId; }

    public String getLocation() {
        return location;
    }

    public double getHumidity() {
        return humidity;
    }

    public double getLat() {
        return lat;
    }

    public String getSensorType() {
        return sensorType;
    }

    public double getLon() {
        return lon;
    }

    public long getTimestamp() { return timestamp; }
    public double getTemperature() { return temperature; }
}

