package query.customtype;

public class AverageWindowsBP {
    
    // The averages of each window
    public Double avg_run;
    public Double avg_walk;
    public Double avg_sleep;
    // The number of elements in each window
    public Integer count_run;
    public Integer count_walk;
    public Integer count_sleep;
    // The length of each window
    public Long last_window_lengh;
    public final Long window_lengh;
    // The time points of windows
    public Long last_timepoint;
    public Long timepoint;

    public AverageWindowsBP(){
        this(0L,5L);
    }

    public AverageWindowsBP(Long start_time, Long window_length){
        this.avg_run = 14.0;
        this.avg_walk = 0D;
        this.avg_sleep = 5.0;
        this.count_run = 0;
        this.count_walk = 0;
        this.count_sleep = 0;
        this.window_lengh = window_length;
        Long curr = start_time;
        this.last_timepoint = curr;
        curr += window_length;
        this.timepoint = curr;
    }

    public void addValue(Integer value, Long timestamp){
        if((timestamp >= this.last_timepoint) && (timestamp < this.timepoint)){
            if (value <= ((this.avg_sleep+this.avg_walk)/2)) {
                this.count_sleep++;
                this.avg_sleep = (this.avg_sleep * this.count_sleep + value) / this.count_sleep;
            } else if (value <= ((this.avg_walk+this.avg_run)/2)) {
                this.count_walk++;
                this.avg_walk = (this.avg_walk * this.count_walk + value) / this.count_walk;
            } else {
                this.count_run++;
                this.avg_run = (this.avg_run * this.count_run + value) / this.count_run;
            }
        } else if (timestamp >= this.timepoint) {
            this.last_timepoint = this.timepoint;
            this.timepoint += this.window_lengh;
            if (value <= ((this.avg_sleep+this.avg_walk)/2)) {
                this.count_sleep = 1;
                this.avg_sleep = (this.avg_sleep * this.count_sleep + value) / this.count_sleep;
            } else if (value <= ((this.avg_walk+this.avg_run)/2)) {
                this.count_walk = 1;
                this.avg_walk = (this.avg_walk * this.count_walk + value) / this.count_walk;
            } else {
                this.count_run = 1;
                this.avg_run = (this.avg_run * this.count_run + value) / this.count_run;
            }
        }
    }

    public String toString(){
        StringBuilder str = new StringBuilder();

        str.append("avg_run: ");
        str.append(this.avg_run).append(" ");
        str.append("\n");

        str.append("count_run: ");
        str.append(this.count_run).append(" ");
        str.append("\n");

        str.append("avg_walk: ");
        str.append(this.avg_walk).append(" ");
        str.append("\n");

        str.append("count_walk: ");
        str.append(this.count_walk).append(" ");
        str.append("\n");

        str.append("avg_sleep: ");
        str.append(this.avg_sleep).append(" ");
        str.append("\n");

        str.append("count_sleep: ");
        str.append(this.count_sleep).append(" ");
        str.append("\n");

        str.append("timepoint: ");
        str.append(this.timepoint).append(" ");
        str.append("\n");

        return str.toString();
    }
}
