package de.tuberlin.io;


import java.util.Locale;

/**
 * Created by patrick on 26.12.16.
 */

import java.util.Locale;


public class TaxiRideClass {


        //private static transient DateTimeFormatter timeFormatter;
        public long rideId;
        public boolean isStart;
        public String startTime;
        public String endTime;
        public float startLon;
        public float startLat;
        public float endLon;
        public float endLat;
        public short passengerCnt;
        public Long timestamp;
        public TaxiRideClass() {
        }

        public TaxiRideClass(long rideId, boolean isStart, String startTime, String endTime, float startLon, float startLat, float endLon, float endLat, short passengerCnt,Long timestamp) {
            this.rideId = rideId;
            this.isStart = isStart;
            this.startTime = startTime;
            this.endTime = endTime;
            this.startLon = startLon;
            this.startLat = startLat;
            this.endLon = endLon;
            this.endLat = endLat;
            this.passengerCnt = passengerCnt;
            this.timestamp=timestamp;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(this.rideId).append(",");
            sb.append(this.isStart?"START":"END").append(",");
            if(this.isStart) {
                sb.append(this.startTime).append(",");
                sb.append(this.endTime).append(",");
            } else {
                sb.append(this.endTime).append(",");
                sb.append(this.startTime).append(",");
            }

            sb.append(this.startLon).append(",");
            sb.append(this.startLat).append(",");
            sb.append(this.endLon).append(",");
            sb.append(this.endLat).append(",");
            sb.append(this.passengerCnt).append(",");
            sb.append(this.timestamp);
            return sb.toString();
        }

        public static TaxiRideClass fromString(String line) {
            String[] tokens = line.split(",");
            if(tokens.length != 10) {
                throw new RuntimeException("Invalid record: " + line);
            } else {
               TaxiRideClass ride = new TaxiRideClass();

                try {
                    ride.rideId = Long.parseLong(tokens[0]);
                    String nfe = tokens[1];
                    byte var4 = -1;
                    switch(nfe.hashCode()) {
                        case 68795:
                            if(nfe.equals("END")) {
                                var4 = 1;
                            }
                            break;
                        case 79219778:
                            if(nfe.equals("START")) {
                                var4 = 0;
                            }
                    }

                    switch(var4) {
                        case 0:
                            ride.isStart = true;
                            ride.startTime = tokens[2];
                            ride.endTime = tokens[3];
                            break;
                        case 1:
                            ride.isStart = false;
                            ride.endTime = tokens[2];
                            ride.startTime = tokens[3];
                            break;
                        default:
                            throw new RuntimeException("Invalid record: " + line);
                    }

                    ride.startLon = tokens[4].length() > 0?Float.parseFloat(tokens[4]):0.0F;
                    ride.startLat = tokens[5].length() > 0?Float.parseFloat(tokens[5]):0.0F;
                    ride.endLon = tokens[6].length() > 0?Float.parseFloat(tokens[6]):0.0F;
                    ride.endLat = tokens[7].length() > 0?Float.parseFloat(tokens[7]):0.0F;
                    ride.passengerCnt = Short.parseShort(tokens[8]);
                    ride.timestamp = tokens[9].length() > 0?Long.parseLong(tokens[9]):0L;
                    return ride;
                } catch (NumberFormatException var5) {
                    throw new RuntimeException("Invalid record: " + line, var5);
                }
            }
        }

        public boolean equals(Object other) {
            return other instanceof TaxiRideClass && this.rideId == ((TaxiRideClass)other).rideId;
        }

        public int hashCode() {
            return (int)this.rideId;
        }



}
