package org.apache.kafka.connect.mixpanel;

import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;

/**
 * Utility class for dealing with dates. Its main purpose is to create intervals of valid dates. For example  to create 3 days intervals for a whole month period.
 * Created by Kostas.
 */
public class DateUtils {

    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String dateRegExFormat = "([0-9]{4})-([0-9]{2})-([0-9]{2})";
    public static final DateTimeFormatter formatter = DateTimeFormat.forPattern(DateUtils.DATE_FORMAT);

    public static String getCurrentDate(){
        return formatter.print(LocalDate.now());
    }

    public static ArrayList<Tuple> getDateIntervals(String from, String to){
        ArrayList<String> dates =  generateDates(from, to);
        ArrayList<Tuple> res = getDateIntervals(dates, dates.size());
        return new ArrayList<>(res.subList(0, res.size() -1));
    }

    public static ArrayList<Tuple> getDateIntervals(ArrayList<String> dates, int n) {
        ArrayList<Tuple> res = new ArrayList<>();

        int len = dates.size() / n;
        int modulo = dates.size() % n;

        if(n > dates.size()){
            for(int i = 0; i < res.size();i++){
                Tuple tuple = new Tuple(dates.get(i), dates.get(i));
                res.add(tuple);
            }
        }else {

            int cur = 0;
            for (int i = 0; i < n; i++) {
                if (i != n - 1) {
                    System.out.println(cur + " : " + (cur + len - 1));
                    Tuple tuple = new Tuple(dates.get(cur), dates.get(cur + len - 1));
                    res.add(tuple);
                } else {
                    System.out.println(cur + " : " + (cur + len - 1 + (modulo)));
                    Tuple tuple = new Tuple(dates.get(cur), dates.get(cur + len - 1 + (modulo)));
                    res.add(tuple);
                }
                cur += len;
            }
        }
        return res;
    }

    public static String addOneDay(String date){
        DateTime dateC = DateTime.parse(date);
        return formatter.print(dateC.plusDays(1));
    }

    public static int compare(String d1, String d2){

        DateTime date1 = DateTime.parse(d1);
        DateTime date2 = DateTime.parse(d2);

        return DateTimeComparator.getDateOnlyInstance().compare(date1, date2);
    }

    public static ArrayList<String> generateDates(String from, String to) {

        ArrayList<String> res = new ArrayList<>();
        DateTime fromDate = DateTime.parse(from);
        DateTime toDate = DateTime.parse(to);


        DateTime tmp = fromDate;

        while(tmp.isBefore(toDate) || tmp.equals(toDate)){
            res.add(formatter.print(tmp));
            tmp = tmp.plusDays(1);
        }

        return res;
    }
}
