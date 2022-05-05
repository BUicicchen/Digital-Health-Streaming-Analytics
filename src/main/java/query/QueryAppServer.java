package query;

import io.undertow.Undertow;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import query.undertow.UndertowHttpHandler;

public final class QueryAppServer {
    public static void main(String[] args) {
        final StatefulFunctions functions = new StatefulFunctions();
        functions.withStatefulFunction(RouterFn.SPEC);
        functions.withStatefulFunction(PatternDetectionFn.SPEC);
        functions.withStatefulFunction(AnomalyDetectionFn.SPEC);
        functions.withStatefulFunction(DBPDetectionFn.SPEC);
        functions.withStatefulFunction(SBPDetectionFn.SPEC);
        functions.withStatefulFunction(GLUDetectionFn.SPEC);
        functions.withStatefulFunction(AlertFn.SPEC);
        functions.withStatefulFunction(AverageBloodPressureFn.SPEC);
        functions.withStatefulFunction(EatingPeriodFn.SPEC);

        final RequestReplyHandler requestReplyHandler = functions.requestReplyHandler();
        final Undertow httpServer =
                Undertow.builder()
                        .addHttpListener(1108, "0.0.0.0")
                        .setHandler(new UndertowHttpHandler(requestReplyHandler))
                        .build();
        httpServer.start();
    }
}
