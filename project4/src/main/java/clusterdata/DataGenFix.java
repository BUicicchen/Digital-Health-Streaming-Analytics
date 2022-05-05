package clusterdata;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.ArrayList;

public class DataGenFix<T> implements ParallelSourceFunction<T> {
    protected static final ArrayList<TupBloodPressure> listBloodPressure = new ArrayList<TupBloodPressure>() {{
        add(new TupBloodPressure(0, 0, 0L, 58, 80));
        add(new TupBloodPressure(0, 0, 1L, 63, 77));
        add(new TupBloodPressure(0, 0, 2L, 59, 76));
        add(new TupBloodPressure(0, 0, 3L, 61, 99));
        add(new TupBloodPressure(0, 0, 4L, 62, 94));
        add(new TupBloodPressure(0, 0, 5L, 48, 97));
        add(new TupBloodPressure(0, 0, 6L, 50, 93));
        add(new TupBloodPressure(0, 0, 7L, 52, 86));
        add(new TupBloodPressure(0, 0, 8L, 53, 89));
        add(new TupBloodPressure(0, 0, 9L, 51, 81));
        add(new TupBloodPressure(0, 0, 10L, 78, 92));
        add(new TupBloodPressure(0, 0, 11L, 78, 87));
        add(new TupBloodPressure(0, 0, 12L, 60, 113));
        add(new TupBloodPressure(0, 0, 13L, 60, 113));
        add(new TupBloodPressure(0, 0, 14L, 55, 97));
        add(new TupBloodPressure(0, 0, 15L, 63, 94));
        add(new TupBloodPressure(0, 0, 16L, 72, 90));
        add(new TupBloodPressure(0, 0, 17L, 77, 98));
        add(new TupBloodPressure(0, 0, 18L, 59, 98));
        add(new TupBloodPressure(0, 0, 19L, 64, 104));
        add(new TupBloodPressure(0, 0, 20L, 64, 116));
        add(new TupBloodPressure(0, 0, 21L, 77, 128));
        add(new TupBloodPressure(0, 0, 22L, 56, 106));
        add(new TupBloodPressure(0, 0, 23L, 56, 117));
        add(new TupBloodPressure(0, 0, 24L, 69, 113));
        add(new TupBloodPressure(0, 0, 25L, 64, 129));
        add(new TupBloodPressure(0, 0, 26L, 79, 139));
        add(new TupBloodPressure(0, 0, 27L, 67, 108));
        add(new TupBloodPressure(0, 0, 28L, 64, 134));
        add(new TupBloodPressure(0, 0, 29L, 55, 136));
        add(new TupBloodPressure(0, 0, 30L, 60, 207));
        add(new TupBloodPressure(0, 0, 31L, 58, 190));
        add(new TupBloodPressure(0, 0, 32L, 61, 206));
        add(new TupBloodPressure(0, 0, 33L, 61, 200));
        add(new TupBloodPressure(0, 0, 34L, 68, 187));
        add(new TupBloodPressure(0, 0, 35L, 59, 197));
        add(new TupBloodPressure(0, 0, 36L, 72, 186));
        add(new TupBloodPressure(0, 0, 37L, 70, 206));
    }};
    protected static final ArrayList<TupSmartwatch> listSmartwatch = new ArrayList<TupSmartwatch>() {{
        add(new TupSmartwatch(4, 0, 0L, 0, 66.71897F));
        add(new TupSmartwatch(4, 0, 1L, 0, 59.700035F));
        add(new TupSmartwatch(4, 0, 2L, 0, 57.56523F));
        add(new TupSmartwatch(4, 0, 3L, 0, 55.09929F));
        add(new TupSmartwatch(4, 0, 4L, 0, 58.603638F));
        add(new TupSmartwatch(4, 0, 5L, 0, 54.380714F));
        add(new TupSmartwatch(4, 0, 6L, 0, 58.77938F));
        add(new TupSmartwatch(4, 0, 7L, 0, 57.421253F));
        add(new TupSmartwatch(4, 0, 8L, 0, 69.65437F));
        add(new TupSmartwatch(4, 0, 9L, 0, 58.181026F));
        add(new TupSmartwatch(4, 0, 10L, 57, 72.549355F));
        add(new TupSmartwatch(4, 0, 11L, 196, 74.92463F));
        add(new TupSmartwatch(4, 0, 12L, 174, 76.36481F));
        add(new TupSmartwatch(4, 0, 13L, 126, 72.816284F));
        add(new TupSmartwatch(4, 0, 14L, 91, 86.21025F));
        add(new TupSmartwatch(4, 0, 15L, 131, 82.48304F));
        add(new TupSmartwatch(4, 0, 16L, 60, 79.129555F));
        add(new TupSmartwatch(4, 0, 17L, 77, 78.245514F));
        add(new TupSmartwatch(4, 0, 18L, 180, 85.11709F));
        add(new TupSmartwatch(4, 0, 19L, 55, 84.36573F));
        add(new TupSmartwatch(4, 0, 20L, 0, 107.79176F));
        add(new TupSmartwatch(4, 0, 21L, 0, 94.307594F));
        add(new TupSmartwatch(4, 0, 22L, 0, 109.67039F));
        add(new TupSmartwatch(4, 0, 23L, 0, 94.826515F));
        add(new TupSmartwatch(4, 0, 24L, 0, 92.47494F));
        add(new TupSmartwatch(4, 0, 25L, 0, 107.653206F));
        add(new TupSmartwatch(4, 0, 26L, 0, 101.26233F));
        add(new TupSmartwatch(4, 0, 27L, 0, 91.794014F));
        add(new TupSmartwatch(4, 0, 28L, 0, 104.886215F));
        add(new TupSmartwatch(4, 0, 29L, 0, 94.74047F));
        add(new TupSmartwatch(4, 0, 30L, 931, 112.23657F));
        add(new TupSmartwatch(4, 0, 31L, 984, 113.565704F));
        add(new TupSmartwatch(4, 0, 32L, 929, 118.66618F));
        add(new TupSmartwatch(4, 0, 33L, 908, 123.675476F));
        add(new TupSmartwatch(4, 0, 34L, 998, 117.74982F));
        add(new TupSmartwatch(4, 0, 35L, 943, 127.54325F));
        add(new TupSmartwatch(4, 0, 36L, 995, 111.19717F));
        add(new TupSmartwatch(4, 0, 37L, 850, 120.40768F));
    }};
    protected static final ArrayList<TupInsulin> listInsulin = new ArrayList<TupInsulin>() {{
        add(new TupInsulin(3, 0, 0L, 0));
        add(new TupInsulin(3, 0, 1L, 0));
        add(new TupInsulin(3, 0, 2L, 0));
        add(new TupInsulin(3, 0, 3L, 0));
        add(new TupInsulin(3, 0, 4L, 0));
        add(new TupInsulin(3, 0, 5L, 0));
        add(new TupInsulin(3, 0, 6L, 0));
        add(new TupInsulin(3, 0, 7L, 0));
        add(new TupInsulin(3, 0, 8L, 0));
        add(new TupInsulin(3, 0, 9L, 0));
        add(new TupInsulin(3, 0, 10L, 0));
        add(new TupInsulin(3, 0, 11L, 0));
        add(new TupInsulin(3, 0, 12L, 0));
        add(new TupInsulin(3, 0, 13L, 0));
        add(new TupInsulin(3, 0, 14L, 0));
        add(new TupInsulin(3, 0, 15L, 0));
        add(new TupInsulin(3, 0, 16L, 0));
        add(new TupInsulin(3, 0, 17L, 0));
        add(new TupInsulin(3, 0, 18L, 0));
        add(new TupInsulin(3, 0, 19L, 0));
        add(new TupInsulin(3, 0, 20L, 0));
        add(new TupInsulin(3, 0, 21L, 0));
        add(new TupInsulin(3, 0, 22L, 0));
        add(new TupInsulin(3, 0, 23L, 0));
        add(new TupInsulin(3, 0, 24L, 0));
        add(new TupInsulin(3, 0, 25L, 0));
        add(new TupInsulin(3, 0, 26L, 0));
        add(new TupInsulin(3, 0, 27L, 0));
        add(new TupInsulin(3, 0, 28L, 0));
        add(new TupInsulin(3, 0, 29L, 0));
        add(new TupInsulin(3, 0, 30L, 21));
        add(new TupInsulin(3, 0, 31L, 5));
        add(new TupInsulin(3, 0, 32L, 3));
        add(new TupInsulin(3, 0, 33L, 6));
        add(new TupInsulin(3, 0, 34L, 3));
        add(new TupInsulin(3, 0, 35L, 13));
        add(new TupInsulin(3, 0, 36L, 4));
        add(new TupInsulin(3, 0, 37L, 13));
    }};
    protected static final ArrayList<TupFitbit> listFitbit = new ArrayList<TupFitbit>() {{
        add(new TupFitbit(1, 0, 0L, 1, 1, 0, 58));
        add(new TupFitbit(1, 0, 1L, 1, 1, 0, 66));
        add(new TupFitbit(1, 0, 2L, 1, 1, 0, 54));
        add(new TupFitbit(1, 0, 3L, 1, 1, 0, 62));
        add(new TupFitbit(1, 0, 4L, 1, 1, 0, 68));
        add(new TupFitbit(1, 0, 5L, 1, 1, 0, 62));
        add(new TupFitbit(1, 0, 6L, 1, 1, 0, 59));
        add(new TupFitbit(1, 0, 7L, 1, 1, 0, 56));
        add(new TupFitbit(1, 0, 8L, 1, 1, 0, 58));
        add(new TupFitbit(1, 0, 9L, 1, 1, 0, 55));
        add(new TupFitbit(1, 0, 10L, 2, 2, 120, 70));
        add(new TupFitbit(1, 0, 11L, 2, 2, 86, 82));
        add(new TupFitbit(1, 0, 12L, 2, 2, 182, 74));
        add(new TupFitbit(1, 0, 13L, 2, 2, 140, 76));
        add(new TupFitbit(1, 0, 14L, 2, 2, 171, 79));
        add(new TupFitbit(1, 0, 15L, 2, 2, 165, 87));
        add(new TupFitbit(1, 0, 16L, 2, 2, 151, 78));
        add(new TupFitbit(1, 0, 17L, 2, 2, 166, 75));
        add(new TupFitbit(1, 0, 18L, 2, 2, 184, 77));
        add(new TupFitbit(1, 0, 19L, 2, 2, 67, 87));
        add(new TupFitbit(1, 0, 20L, 1, 1, 0, 106));
        add(new TupFitbit(1, 0, 21L, 1, 1, 0, 100));
        add(new TupFitbit(1, 0, 22L, 1, 1, 0, 107));
        add(new TupFitbit(1, 0, 23L, 1, 1, 0, 102));
        add(new TupFitbit(1, 0, 24L, 1, 1, 0, 96));
        add(new TupFitbit(1, 0, 25L, 1, 1, 0, 109));
        add(new TupFitbit(1, 0, 26L, 1, 1, 0, 100));
        add(new TupFitbit(1, 0, 27L, 1, 1, 0, 93));
        add(new TupFitbit(1, 0, 28L, 1, 1, 0, 108));
        add(new TupFitbit(1, 0, 29L, 1, 1, 0, 100));
        add(new TupFitbit(1, 0, 30L, 3, 3, 990, 126));
        add(new TupFitbit(1, 0, 31L, 3, 3, 941, 118));
        add(new TupFitbit(1, 0, 32L, 3, 3, 987, 124));
        add(new TupFitbit(1, 0, 33L, 3, 3, 868, 110));
        add(new TupFitbit(1, 0, 34L, 3, 3, 872, 115));
        add(new TupFitbit(1, 0, 35L, 3, 3, 885, 127));
        add(new TupFitbit(1, 0, 36L, 3, 3, 986, 113));
        add(new TupFitbit(1, 0, 37L, 3, 3, 963, 121));
    }};
    protected static final ArrayList<TupGlucose> listGlucose = new ArrayList<TupGlucose>() {{
        add(new TupGlucose(1, 0, 0L, 115));
        add(new TupGlucose(1, 0, 1L, 118));
        add(new TupGlucose(1, 0, 2L, 126));
        add(new TupGlucose(1, 0, 3L, 112));
        add(new TupGlucose(1, 0, 4L, 119));
        add(new TupGlucose(1, 0, 5L, 130));
        add(new TupGlucose(1, 0, 6L, 111));
        add(new TupGlucose(1, 0, 7L, 130));
        add(new TupGlucose(1, 0, 8L, 136));
        add(new TupGlucose(1, 0, 9L, 128));
        add(new TupGlucose(1, 0, 10L, 94));
        add(new TupGlucose(1, 0, 11L, 78));
        add(new TupGlucose(1, 0, 12L, 109));
        add(new TupGlucose(1, 0, 13L, 102));
        add(new TupGlucose(1, 0, 14L, 105));
        add(new TupGlucose(1, 0, 15L, 98));
        add(new TupGlucose(1, 0, 16L, 94));
        add(new TupGlucose(1, 0, 17L, 82));
        add(new TupGlucose(1, 0, 18L, 90));
        add(new TupGlucose(1, 0, 19L, 70));
        add(new TupGlucose(1, 0, 20L, 274));
        add(new TupGlucose(1, 0, 21L, 306));
        add(new TupGlucose(1, 0, 22L, 227));
        add(new TupGlucose(1, 0, 23L, 283));
        add(new TupGlucose(1, 0, 24L, 295));
        add(new TupGlucose(1, 0, 25L, 181));
        add(new TupGlucose(1, 0, 26L, 182));
        add(new TupGlucose(1, 0, 27L, 168));
        add(new TupGlucose(1, 0, 28L, 174));
        add(new TupGlucose(1, 0, 29L, 317));
        add(new TupGlucose(1, 0, 30L, 115));
        add(new TupGlucose(1, 0, 31L, 102));
        add(new TupGlucose(1, 0, 32L, 127));
        add(new TupGlucose(1, 0, 33L, 117));
        add(new TupGlucose(1, 0, 34L, 113));
        add(new TupGlucose(1, 0, 35L, 133));
        add(new TupGlucose(1, 0, 36L, 126));
        add(new TupGlucose(1, 0, 37L, 138));
    }};
    protected String type;
    public DataGenFix(String type) {
        this.type = type;
    }
    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        switch (type) {
            case TupBloodPressure.deviceName:
                for (TupBloodPressure event : listBloodPressure) {
                    // noinspection unchecked
                    T eventConv = (T) event;
                    ctx.collectWithTimestamp(eventConv, event.timestamp);
                }
                break;
            case TupSmartwatch.deviceName:
                for (TupSmartwatch event : listSmartwatch) {
                    // noinspection unchecked
                    T eventConv = (T) event;
                    ctx.collectWithTimestamp(eventConv, event.timestamp);
                }
                break;
            case TupInsulin.deviceName:
                for (TupInsulin event : listInsulin) {
                    // noinspection unchecked
                    T eventConv = (T) event;
                    ctx.collectWithTimestamp(eventConv, event.timestamp);
                }
                break;
            case TupFitbit.deviceName:
                for (TupFitbit event : listFitbit) {
                    // noinspection unchecked
                    T eventConv = (T) event;
                    ctx.collectWithTimestamp(eventConv, event.timestamp);
                }
                break;
            case TupGlucose.deviceName:
                for (TupGlucose event : listGlucose) {
                    // noinspection unchecked
                    T eventConv = (T) event;
                    ctx.collectWithTimestamp(eventConv, event.timestamp);
                }
                break;
        }
    }
    @Override
    public void cancel() {}
}
