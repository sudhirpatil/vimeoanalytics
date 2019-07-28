package com.sp.parrot;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Utils {

    static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00");
    static DateTimeFormatter dateTimeKey = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

    public static String getDateTimeHour(){
        return ZonedDateTime.now(ZoneId.of("UTC")).format(dateTimeFormatter);
    }

    public static String getDateTimeKey(){
        return ZonedDateTime.now(ZoneId.of("UTC")).format(dateTimeKey);
    }
}
