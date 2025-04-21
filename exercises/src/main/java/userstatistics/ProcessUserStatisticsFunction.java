package userstatistics;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import models.UserStatistics;

import java.util.Objects;

public class ProcessUserStatisticsFunction
        extends ProcessWindowFunction<UserStatistics, UserStatistics, String, TimeWindow> {

    private ValueStateDescriptor<UserStatistics> stateDescriptor;

    @Override
    public void open(Configuration config) throws Exception {
        stateDescriptor = new ValueStateDescriptor<UserStatistics>("User Statistics", UserStatistics.class);
        super.open(config);
    }

    @Override
    public void process(
            String emailAddress,
            ProcessWindowFunction<UserStatistics, UserStatistics, String, TimeWindow>.Context ctx,
            Iterable<UserStatistics> iter,
            Collector<UserStatistics> out
            ) throws Exception {

        ValueState<UserStatistics> state = ctx.globalState().getState(stateDescriptor);
        UserStatistics userStats = state.value();

        for (UserStatistics incomingUserStats : iter) {
            if (Objects.isNull(userStats)){
                userStats = incomingUserStats;
            }
            else {
                userStats = userStats.merge(incomingUserStats);
            }
        }

        state.update(userStats);
        out.collect(userStats);
    }
}