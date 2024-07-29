package example;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;

public class TaxiRide {
    private String color;
    private double vendorID;
    private Date pickupDatetime;
    private Date dropoffDatetime;
    private double distance;
    private double totalAmount;
    private double congestion_surcharge;
    public TaxiRide() {}

    public TaxiRide(String color, double vendorID, String pickupDatetime, String dropoffDatetime, double distance, double totalAmount, double congestion_surcharge) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.color = color;
        this.vendorID = vendorID;
        this.pickupDatetime = sdf.parse(pickupDatetime);
        this.dropoffDatetime = sdf.parse(dropoffDatetime);
        this.distance = distance;
        this.totalAmount = totalAmount;
        this.congestion_surcharge = congestion_surcharge;
    }

    // Getters
    public String getColor() {
        return color;
    }

    public double getVendorID() {
        return vendorID;
    }

    public Date getPickupDatetime() {
        return pickupDatetime;
    }

    public Date getDropoffDatetime() {
        return dropoffDatetime;
    }

    public double getDistance() {
        return distance;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public double getCongestionSurcharge() {
        return congestion_surcharge;
    }

    // Setters
    public void setColor(String color) {
        this.color = color;
    }

    public void setVendorID(double vendorID) {
        this.vendorID = vendorID;
    }

    public void setPickupDatetime(String pickupDatetime) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.pickupDatetime = sdf.parse(pickupDatetime);
    }

    public void setDropoffDatetime(String dropoffDatetime) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.dropoffDatetime = sdf.parse(dropoffDatetime);
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public void setCongestionSurcharge(double congestion_surcharge) {
        this.congestion_surcharge = congestion_surcharge;
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return "TaxiRide{" +
                "color='" + color + '\'' +
                ", vendorID=" + vendorID +
                ", pickupDatetime=" + sdf.format(pickupDatetime) +
                ", dropoffDatetime=" + sdf.format(dropoffDatetime) +
                ", distance=" + distance +
                ", totalAmount=" + totalAmount +
                ", congestion_surcharge=" + congestion_surcharge +
                '}';
    }
}
