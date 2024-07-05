package example;

import java.util.Date;
import java.text.SimpleDateFormat;
import  java.text.ParseException;
public class TaxiRide {
    private String color;
    private double vendorID;
    private Date pickupDatetime;
    private Date dropoffDatetime;
    private double distance;
    private double totalAmount;
    private double congestion_surcharge;

    public TaxiRide(String color, double vendorID, String pickupDatetime, String dropoffDatetime, double distance, double totalAmount,double congestion_surcharge) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.color = color;
        this.vendorID = vendorID;
        this.pickupDatetime = sdf.parse(pickupDatetime);
        this.dropoffDatetime = sdf.parse(dropoffDatetime);
        this.distance = distance;
        this.totalAmount = totalAmount;
        this.congestion_surcharge = congestion_surcharge;
    }

    public String getColor(){
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

}
