package models;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.ZonedDateTime;
import java.util.Objects;

import models.FlightData;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SunsetAirFlightData {

    private String customerEmailAddress;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    private ZonedDateTime departureTime;
    private String departureAirport;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    private ZonedDateTime arrivalTime;
    private String arrivalAirport;
    private String flightId;
    private String referenceNumber;

    public void SunsetAirFlightData(){};

    public void setCustomerEmailAddress(String customerEmailAddress) {
        this.customerEmailAddress = customerEmailAddress;
    }

    public String getCustomerEmailAddress() {
        return customerEmailAddress;
    }

    public void setDepartureTime(ZonedDateTime departureTime) {
        this.departureTime = departureTime;
    }

    public ZonedDateTime getDepartureTime() {
        return departureTime;
    }

    public void setDepartureAirport(String departureAirport) {
        this.departureAirport = departureAirport;
    }

    public String getDepartureAirport() {
        return departureAirport;
    }

    public void setArrivalTime(ZonedDateTime arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    public ZonedDateTime getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalAirport(String arrivalAirport) {
        this.arrivalAirport = arrivalAirport;
    }

    public String getArrivalAirport() {
        return arrivalAirport;
    }

    public void setFlightId(String flightId) {
        this.flightId = flightId;
    }

    public String getFlightId() {
        return flightId;
    }

    public void setReferenceNumber(String referenceNumber) {
        this.referenceNumber = referenceNumber;
    }

    public String getReferenceNumber() {
        return referenceNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SunsetAirFlightData that = (SunsetAirFlightData) o;
        return Objects.equals(customerEmailAddress, that.customerEmailAddress) && Objects.equals(departureTime, that.departureTime) && Objects.equals(departureAirport, that.departureAirport) && Objects.equals(arrivalTime, that.arrivalTime) && Objects.equals(arrivalAirport, that.arrivalAirport) && Objects.equals(flightId, that.flightId) && Objects.equals(referenceNumber, that.referenceNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customerEmailAddress, departureTime, departureAirport, arrivalTime, arrivalAirport, flightId, referenceNumber);
    }

    @Override
    public String toString() {
        return "SunsetAirFlightData{" +
                "customerEmailAddress='" + customerEmailAddress + '\'' +
                ", departureTime=" + departureTime +
                ", departureAirport='" + departureAirport + '\'' +
                ", arrivalTime=" + arrivalTime +
                ", arrivalAirport='" + arrivalAirport + '\'' +
                ", flightId='" + flightId + '\'' +
                ", referenceNumber='" + referenceNumber + '\'' +
                '}';
    }

    public FlightData toFlightData(){
        FlightData flightData = new FlightData();
        flightData.setEmailAddress(this.customerEmailAddress);
        flightData.setDepartureTime(this.departureTime);
        flightData.setDepartureAirportCode(this.departureAirport);
        flightData.setArrivalTime(this.arrivalTime);
        flightData.setArrivalAirportCode(this.arrivalAirport);
        flightData.setFlightNumber(this.flightId);
        flightData.setConfirmationCode(this.referenceNumber);
        return flightData;
    }
}