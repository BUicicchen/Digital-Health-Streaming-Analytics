package query.customtype;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class AverageWindows {
//    private static final ObjectMapper mapper = new ObjectMapper();
//
//    public static final Type<AverageWindows> TYPE = SimpleType.simpleImmutableTypeFrom(
//            TypeName.typeNameFromString("query.customtype/averagewindows"),
//            mapper::writeValueAsBytes,
//    bytes ->mapper.readValue(bytes,AverageWindows.class));

    // The number of windows
    @JsonProperty("window_size")
    public final Integer size;

    // The averages of each window
    @JsonProperty("window_avg")
    public final List<Double> avg;

    // The number of elements in each window
    @JsonProperty("window_ele")
    public final List<Integer> count;

    // The length of each window
    @JsonProperty("window_length")
    public final Long window_lengh;

    // The time points of windows
    @JsonProperty("window_timepoint")
    public final List<Long> timepoint;

    public AverageWindows(){
        this(3,0L,5L);
    }

    public AverageWindows(Integer windows_num, Long start_time, Long window_length){
        this.size = windows_num;
        this.avg = new ArrayList<Double>();
        this.count = new ArrayList<Integer>();
        this.timepoint = new ArrayList<Long>();
        this.window_lengh = window_length;
        Long curr = start_time;
        for(int i = 0; i < windows_num; i++){
            this.avg.add(0D);
            this.count.add(0);
            this.timepoint.add(curr);
            curr += window_length;
        }
        this.timepoint.add(curr);

    }

    public void addValue(Integer value, Integer index){
        Integer i = index;
        if(i < 0) {return;}
        else if(i >= this.size) {
            moveForward();
            i -= 1;
        }

        Integer count = this.count.get(i);
        Double avg = this.avg.get(i);
        this.avg.set(i, (avg * count + value) / ++count);
        this.count.set(i, count);
    }

    public Integer findWindowIndex(Long timestamp){
        // timestamp less than the smallest time
        if(timestamp < this.timepoint.get(0)){return -1;}

        for(int i = 0;i < this.size; i++){
            if((timestamp >= this.timepoint.get(i)) && (timestamp < this.timepoint.get(i+1))){
                return i;
            }
        }
        // timestamp greater than the largest time
        return this.size;
    }

    public void moveForward(){
//        for(int i = 0; i < size-1; i++){
//            this.count.set(i, this.count.get(i+1));
//            this.avg.set(i, this.avg.get(i+1));
//            this.timepoint.set(i, this.timepoint.get(i) + this.window_lengh);
//        }
//        this.count.set(size-1, 0);
//        this.avg.set(size-1, 0D);
//        this.timepoint.set(size-1, this.timepoint.get(size-1) + this.window_lengh);
//        this.timepoint.set(size, this.timepoint.get(size) + this.window_lengh);
        this.count.remove(0);
        this.count.add(0);

        this.avg.remove(0);
        this.avg.add(0D);

        this.timepoint.remove(0);
        this.timepoint.add(this.timepoint.get(this.size-2)+this.window_lengh);
    }

    public Double[] getAvg(){
        if(this.count.get(this.size-1) <= 0){return null;}
        Double[] result = new Double[2];
        Double vlu = 0D;
        Integer cnt = 0;
        for(int i = 0; i < this.size; i++){
            vlu += this.avg.get(i) * this.count.get(i);
            cnt += this.count.get(i);
        }
        result[0] = vlu/cnt;
        result[1] = this.avg.get(this.size-1);
        return result;

    }

    public String toString(){
        StringBuilder str = new StringBuilder();
        str.append("size: ").append(this.size).append("\n");

        str.append("avg: ");
        for (Double aDouble : this.avg) {
            str.append(aDouble).append(" ");
        }
        str.append("\n");

        str.append("count: ");
        for (Integer integer : this.count) {
            str.append(integer).append(" ");
        }
        str.append("\n");

        str.append("avg: ");
        for (Long aLong : this.timepoint) {
            str.append(aLong).append(" ");
        }
        str.append("\n");

        return str.toString();


    }


}
