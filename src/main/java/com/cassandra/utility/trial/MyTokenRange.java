package com.cassandra.utility.trial;

import java.util.Comparator;


public class MyTokenRange implements Comparable {
    private final Long start;
    private final Long end;

    public MyTokenRange(Long start, Long end) {
        this.start = start;
        this.end = end;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MyTokenRange that = (MyTokenRange) o;

        if (start != null ? !start.equals(that.start) : that.start != null) return false;
        return end != null ? end.equals(that.end) : that.end == null;

    }

    @Override
    public int hashCode() {
        int result = start != null ? start.hashCode() : 0;
        result = 31 * result + (end != null ? end.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MyTokenRange{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        MyTokenRange that = (MyTokenRange)o;
        return 0;
    }

    /*class MyTokenRangeCompare implements Comparator<MyTokenRange> {
        @Override
        public int compare(MyTokenRange o1, MyTokenRange o2) {
            if(o1.getStart()<o2.getStart())
                return 1;
            else
                return -1;
        }
    }*/
}
