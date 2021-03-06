package com.flyai.recommend;

import com.alibaba.fastjson.JSON;
import com.flyai.recommend.entity.RedisActionEntity;
import com.flyai.recommend.entity.SensorsEntity;
import com.flyai.recommend.entity.UserEntity;
import com.flyai.recommend.entity.UserVectorEntity;
import com.flyai.recommend.enums.BrandEnums;
import com.flyai.recommend.enums.ConstellationEnums;
import com.flyai.recommend.enums.RedisActionEnums;
import com.flyai.recommend.mapper.MyMongodbSink2;
import com.flyai.recommend.mapper.MyRedisSink3;
import com.flyai.recommend.mapper.MyTiDbSink;
import com.flyai.recommend.utils.HttpClient;
import com.flyai.recommend.utils.OneHotUtils;
import com.flyai.recommend.utils.RedisUtils;
import com.flyai.recommend.utils.VectorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author lizhe
 */
@Slf4j
public class UserPortraitApplication {
    private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static StreamExecutionEnvironment env;
    private static DataStream <String> dataStream;
    static Map <String, String> cityMap = new HashMap <>();

    static {
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "0");
        cityMap.put("?????????", "5");
        cityMap.put("???????????????????????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("????????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("????????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("????????????????????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("???????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("???????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("??????????????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "0");
        cityMap.put("?????????", "3");
        cityMap.put("????????????????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????????????????????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("??????????????????????????????", "4");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "1");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("????????????", "2");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("????????????????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????????????????", "5");
        cityMap.put("??????????????????????????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "6");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "2");
        cityMap.put("??????????????????????????????", "5");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????????????????", "5");
        cityMap.put("?????????????????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("????????????", "4");
        cityMap.put("?????????", "3");
        cityMap.put("???????????????????????????", "4");
        cityMap.put("????????????", "2");
        cityMap.put("???????????????", "4");
        cityMap.put("???????????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "2");
        cityMap.put("???????????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "3");
        cityMap.put("????????????", "3");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("????????????", "5");
        cityMap.put("??????", "6");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "3");
        cityMap.put("???????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "2");
        cityMap.put("???????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????????????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("???????????????????????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "3");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "1");
        cityMap.put("???????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("????????????", "4");
        cityMap.put("?????????", "0");
        cityMap.put("?????????", "4");
        cityMap.put("????????????", "5");
        cityMap.put("????????????", "4");
        cityMap.put("?????????????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("????????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "0");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "3");
        cityMap.put("???????????????????????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????????????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????????????????????????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("??????????????????????????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????????????????", "5");
        cityMap.put("??????", "6");
        cityMap.put("???????????????????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????????????????", "5");
        cityMap.put("?????????????????????", "5");
        cityMap.put("???????????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("???????????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("???????????????", "4");
        cityMap.put("??????????????????????????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "1");
        cityMap.put("?????????????????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("????????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "4");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("??????????????????????????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "5");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "1");
        cityMap.put("?????????", "2");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "2");
        cityMap.put("?????????????????????", "5");
        cityMap.put("????????????", "4");
        cityMap.put("?????????", "3");
        cityMap.put("?????????????????????", "5");
        cityMap.put("????????????", "4");
        cityMap.put("?????????", "5");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "2");
        cityMap.put("??????????????????????????????", "5");
        cityMap.put("?????????", "3");
        cityMap.put("?????????", "4");
        cityMap.put("?????????", "3");
        cityMap.put("????????????", "5");
        cityMap.put("?????????", "2");
    }

    public static void main(String[] args) {
        try {
            setEnv();
            setCheckPoint();
            setSource();
            vectorCalculate();
            env.execute("user vector job start");
        } catch (Exception e) {
            log.error("job run exception:{}", Throwables.getStackTraceAsString(e));
        }
    }

    private static void setEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
    }

    private static void setCheckPoint() {
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    private static void setSource() throws IOException {
        Properties properties = new Properties();
        properties.load(RecommendApplication.class.getResourceAsStream("/application.properties"));
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", properties.getProperty("kafka.event.sensors.servers"));
        kafkaProperties.setProperty("group.id", properties.getProperty("kafka.event.consumer.sensors.group"));
        kafkaProperties.setProperty("enable.auto.commit", "false");
        String topics = properties.getProperty("kafka.event.consumer.sensors.topic");
        FlinkKafkaConsumer <String> consumer = new FlinkKafkaConsumer <String>(topics, new SimpleStringSchema(), kafkaProperties);
        consumer.setStartFromGroupOffsets();
        dataStream = env.addSource(consumer);
    }

    private static void vectorCalculate() throws Exception {
        OutputTag <RedisActionEntity> redisActionEntityOutputTag = new OutputTag <RedisActionEntity>("redisTag") {};
        OutputTag <UserVectorEntity> userVectorEntityOutputTag = new OutputTag <UserVectorEntity>("vectorTag") {};
        SingleOutputStreamOperator <UserEntity> baseStream = dataStream
                .filter(new FilterFunction <String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        if (s == null
                                || s.isEmpty()) {
                            return false;
                        }
                        return true;
                    }
                })
                .map(new MapFunction <String, SensorsEntity>() {
                    @Override
                    public SensorsEntity map(String s) throws Exception {
                        try {
                            return JSON.parseObject(s, SensorsEntity.class);
                        } catch (Exception e) {
                            log.error("transform failed:{}", e.getMessage());
                        }
                        return null;
                    }
                })
                .flatMap(new FlatMapFunction <SensorsEntity, SensorsEntity>() {
                    @Override
                    public void flatMap(SensorsEntity sensorsEntity, Collector <SensorsEntity> collector) throws Exception {
                        if (sensorsEntity != null) {
                            if (sensorsEntity.getType() != null
                                    && sensorsEntity.getType().equals("profile_set")) {
                                collector.collect(sensorsEntity);
                            }

                            if (sensorsEntity.getEvent() != null
                                    && sensorsEntity.getEvent().equals("$AppStart")
                            ) {
                                collector.collect(sensorsEntity);
                            }

                            if (sensorsEntity.getEvent() != null
                                    && sensorsEntity.getEvent().equals("nodeClick")
                            ) {
                                Map <String, String> properties = JSON.parseObject(sensorsEntity.getProperties(), Map.class);
                                if (properties != null
                                        && properties.containsKey("node_name")
                                        && (properties.get("node_name").equals("??????")
                                        || properties.get("node_name").equals("??????")
                                )
                                ) {
                                    collector.collect(sensorsEntity);
                                }
                            }
                        }
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor <SensorsEntity>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(SensorsEntity sensorsEntity) {
                        return sensorsEntity.getTime();
                    }
                })
                .flatMap(new FlatMapFunction <SensorsEntity, UserEntity>() {
                    @Override
                    public void flatMap(SensorsEntity sensorsEntity, Collector <UserEntity> collector) throws Exception {
                        UserEntity userEntity = new UserEntity();
                        userEntity.setId(sensorsEntity.getDistinctId());
                        String properties = sensorsEntity.getProperties();
                        userEntity.setStaff(0);
                        userEntity.setAction_time(sensorsEntity.getTime());
                        if (sensorsEntity.getEvent() != null
                                && sensorsEntity.getEvent().equals("nodeClick")) {
                            userEntity.setStaff(1);
                        }
                        if (properties != null) {
                            Map <String, Object> map = JSON.parseObject(properties, Map.class);
                            if (map != null) {
                                if (map.containsKey("nickname")) {
                                    userEntity.setNick_name(map.get("nickname").toString());
                                }
                                if (map.containsKey("born_date") && !map.get("born_date").toString().isEmpty()) {
                                    Date parse = df.parse(map.get("born_date").toString());
                                    long timestamp = parse.getTime();
                                    if (!String.valueOf(timestamp).isEmpty()) {
                                        userEntity.setBirthday(timestamp);
                                    }
                                }
                                if (map.containsKey("place")) {
                                    userEntity.setLocation(map.get("place").toString());
                                }
                                if (map.containsKey("cplace")) {
                                    userEntity.setBirthplace(map.get("cplace").toString());
                                }
                                if (map.containsKey("sun")) {
                                    userEntity.setSun(map.get("sun").toString());
                                }
                                if (map.containsKey("sex")) {
                                    userEntity.setSex(map.get("sex").toString());
                                }
                                if (map.containsKey("is_vip")) {
                                    userEntity.setIs_vip(Integer.valueOf(map.get("is_vip").toString()));
                                }
                                if (map.containsKey("register_time")) {
                                    long timestamp = 0L;
                                    String registerTime = map.get("register_time").toString();
                                    if (registerTime.contains("-")) {
                                        Date parse = df.parse(registerTime);
                                        timestamp = parse.getTime();
                                    } else {
                                        timestamp = Long.parseLong(map.get("register_time").toString());
                                    }
                                    userEntity.setRegister_time(timestamp);
                                }
                                if (map.containsKey("$country")) {
                                    userEntity.setCountry(map.get("$country").toString());
                                }
                                if (map.containsKey("$province")) {
                                    userEntity.setProvince(map.get("$province").toString());
                                }
                                if (map.containsKey("$city")) {
                                    userEntity.setCity(map.get("$city").toString());
                                }
                                if (map.containsKey("$manufacturer")) {
                                    userEntity.setManufacturer(map.get("$manufacturer").toString());
                                }
                                if (map.containsKey("$os")) {
                                    userEntity.setOs(map.get("$os").toString());
                                }
                                if (map.containsKey("phone_number") && !map.get("phone_number").toString().isEmpty()) {
                                    userEntity.setPhone_number(map.get("phone_number").toString());
                                }
                                if (map.containsKey("$device_id")) {
                                    userEntity.setDevice_id(map.get("$device_id").toString());
                                }
                                if (map.containsKey("$screen_height")) {
                                    userEntity.setScreen_height(map.get("$screen_height").toString());
                                }
                                if (map.containsKey("$screen_width")) {
                                    userEntity.setScreen_width(map.get("$screen_width").toString());
                                }
                                if (map.containsKey("$network_type")) {
                                    userEntity.setNetwork_type(map.get("$network_type").toString());
                                }
                                if (map.containsKey("$carrier")) {
                                    userEntity.setCarrier(map.get("$carrier").toString());
                                }
                                if (map.containsKey("pay_money")) {
                                    userEntity.setPay(map.get("pay_money").toString());
                                }
                                if (map.containsKey("$ip")) {
                                    userEntity.setIp(map.get("$ip").toString());
                                }
                            }
                        }
                        collector.collect(userEntity);
                    }
                })
                .keyBy("id")
                .timeWindow(Time.minutes(5))
                .reduce(new ReduceFunction <UserEntity>() {
                    @Override
                    public UserEntity reduce(UserEntity userEntity, UserEntity t1) throws Exception {
                        UserEntity merge = userEntity;
                        if (userEntity.getId().equals(t1.getId())) {
                            if (userEntity.getAction_time() > t1.getAction_time()) {
                                merge = merge(t1, userEntity);
                            } else {
                                merge = merge(userEntity, t1);
                            }
                        }
                        return merge;
                    }
                })
                .process(new ProcessFunction <UserEntity, UserEntity>() {
                    @Override
                    public void processElement(UserEntity userEntity, Context context, Collector <UserEntity> collector) throws Exception {
                        if (userEntity.getIp() != null && !userEntity.getIp().isEmpty()) {
                            String redisKey = "ipPool:" + userEntity.getIp();
                            Map <String, String> ipInfo = new HashMap <>();
                            ipInfo.put("action", RedisActionEnums.HGetAll.value().toString());
                            ipInfo.put("key", redisKey);
                            Object o = RedisUtils.outputProcess(ipInfo);
                            if (o == null) {
                                try {
                                    String url = "http://whois.pconline.com.cn/ipJson.jsp?json=true&ip=" + userEntity.getIp();
                                    String s = HttpClient.doGet(url);
                                    Map <String, String> cityMap = JSON.parseObject(s, Map.class);
                                    if (cityMap != null && cityMap.containsKey("city")) {
                                        userEntity.setCity(cityMap.get("city"));
                                    }
                                    RedisActionEntity redisActionEntity = new RedisActionEntity(RedisActionEnums.HMSet.value(), redisKey, null, s);
                                    context.output(redisActionEntityOutputTag, redisActionEntity);
                                } catch (Exception e) {
                                    log.error("get city by API failed:{}", e.getMessage());
                                }
                            } else {
                                System.err.println("get from redis");
                                Map <String, String> cityMap = JSON.parseObject(JSON.toJSONString(o), Map.class);
                                if (cityMap != null && cityMap.containsKey("city")) {
                                    userEntity.setCity(cityMap.get("city"));
                                }
                            }
                        }
                        UserVectorEntity userVector = getUserVector(userEntity);
                        context.output(userVectorEntityOutputTag, userVector);
                    collector.collect(userEntity);
                    }
                });
        // ??????????????????
        baseStream.addSink(new MyTiDbSink <>());
        // ??????ip?????????
        DataStream <RedisActionEntity> redisOutput = baseStream.getSideOutput(redisActionEntityOutputTag);
        FlinkJedisPoolConfig redisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("10.19.52.120")
                .setMaxTotal(5000)
                .setMaxIdle(10)
                .setMinIdle(5)
                .setPort(6379)
                .setPassword("cece_xxwolo")
                .build();
        redisOutput.addSink(new MyRedisSink3 <>(redisPoolConfig));
        // ????????????????????????
        DataStream <UserVectorEntity> userVectorOutput = baseStream.getSideOutput(userVectorEntityOutputTag);
        userVectorOutput.addSink(new MyTiDbSink <>());
    }

    private static UserEntity merge(UserEntity merge, UserEntity beMerged) {
        if (merge != null && beMerged != null) {
            Map <String, Object> mergeMap = JSON.parseObject(JSON.toJSONString(merge), Map.class);
            Map <String, Object> beMergedMap = JSON.parseObject(JSON.toJSONString(beMerged), Map.class);
            for (Map.Entry <String, Object> entry : beMergedMap.entrySet()) {
                mergeMap.put(entry.getKey(), entry.getValue());
            }
            merge = JSON.parseObject(JSON.toJSONString(mergeMap), UserEntity.class);
        }
        return merge == null ? beMerged : merge;
    }

    private static UserVectorEntity getUserVector(UserEntity userEntity) {
        UserVectorEntity userVectorEntity = new UserVectorEntity();
        userVectorEntity.setId(userEntity.getId());
        // @todo ??????
        if (userEntity.getBirthday() != null) {
            String ageVector = age(userEntity.getBirthday());
            if (ageVector != null) {
                userVectorEntity.setBirthday_vector(ageVector);
            }
        }
        // @todo ????????????
        if (userEntity.getSex() != null) {
            String sexVector = sex(userEntity.getSex());
            if (sexVector != null) {
                userVectorEntity.setSex_vector(sexVector);
            }
        }
        // @todo ??????????????????
        if (userEntity.getSun() != null) {
            String sunVector = sun(userEntity.getSun());
            if (sunVector != null) {
                userVectorEntity.setSun_vector(sunVector);
            }
        }
        // @todo ????????????
        if (userEntity.getIs_vip() != null) {
            String vipVector = vip(userEntity.getIs_vip());
            if (vipVector != null) {
                userVectorEntity.setIs_vip_vector(vipVector);
            }
        }
        // @todo ????????????????????????
        if (userEntity.getRegister_time() != null) {
            String registerTimeVector = VectorUtils.week(String.valueOf(userEntity.getRegister_time()));
            if (registerTimeVector != null) {
                userVectorEntity.setRegister_time_vector(registerTimeVector);
            }
        }
        // @todo ????????????????????????
        userVectorEntity.setAction_time_vector(VectorUtils.week(String.valueOf(new Date().getTime())));
        // @todo ??????????????????
        if (userEntity.getCity() != null) {
            String cityVector = city(userEntity.getCity());
            if (cityVector != null) {
                userVectorEntity.setCity_vector(cityVector);
            }
        }
        // @todo ??????????????????
        if (userEntity.getManufacturer() != null) {
            String manufacturerVector = manufacturer(userEntity.getManufacturer());
            if (manufacturerVector != null) {
                userVectorEntity.setManufacturer_vector(manufacturerVector);
            }
        }
        // @todo ??????????????????
        if (userEntity.getOs() != null) {
            String osVector = os(userEntity.getOs());
            if (osVector != null) {
                userVectorEntity.setOs_vector(osVector);
            }
        }
        // @todo ????????????minmax
        if (userEntity.getScreen_height() != null) {
            String screenHeightMinMax = screenHeight(userEntity.getScreen_height());
            if (screenHeightMinMax != null) {
                userVectorEntity.setScreen_height_vector(screenHeightMinMax);
            }
        }
        // @todo ????????????minmax
        if (userEntity.getScreen_width() != null) {
            String screenWidthMinMax = screenWidth(userEntity.getScreen_width());
            if (screenWidthMinMax != null) {
                userVectorEntity.setScreen_width_vector(screenWidthMinMax);
            }
        }
        // @todo ??????????????????
        if (userEntity.getNetwork_type() != null) {
            String networkTypeVector = networkType(userEntity.getNetwork_type());
            if (networkTypeVector != null) {
                userVectorEntity.setNetwork_type_vector(networkTypeVector);
            }
        }
        // @todo ???????????????
        if (userEntity.getCarrier() != null) {
            String carrierVector = carrier(userEntity.getCarrier());
            if (carrierVector != null) {
                userVectorEntity.setCarrier_vector(carrierVector);
            }
        }
        // @todo ??????????????????
        if (userEntity.getStaff() != null) {
            String staffVector = isStaff(userEntity.getStaff());
            if (staffVector != null) {
                userVectorEntity.setStaff_vector(staffVector);
            }
        }
        // @todo ??????????????????
        if (userEntity.getPay() != null) {
            String payVector = pay(userEntity.getPay());
            if (payVector != null) {
                userVectorEntity.setPay_vector(payVector);
            }
        }
        return userVectorEntity;
    }

    private static String age(Long birthday) {
        try {
            Date bDate = new Date(birthday);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(bDate);
            int birthYear = calendar.getWeekYear();
            calendar.setTime(new Date());
            int nowYear = calendar.getWeekYear();
            int age = nowYear - birthYear;
            if (age <= 10) {
                return OneHotUtils.getOneHotCode(6, 0);
            }
            if (age <= 20) {
                return OneHotUtils.getOneHotCode(6, 1);
            }
            if (age <= 25) {
                return OneHotUtils.getOneHotCode(6, 2);
            }
            if (age <= 35) {
                return OneHotUtils.getOneHotCode(6, 3);
            }
            if (age <= 45) {
                return OneHotUtils.getOneHotCode(6, 4);
            }
            return OneHotUtils.getOneHotCode(6, 5);
        } catch (Exception e) {
            log.error("get age vector failed:{}", e.getMessage());
        }
        return null;
    }

    private static String sex(String sex) {
        if (sex == null
                || sex.isEmpty()) {
            return null;
        }

        if (sex.equals("???")) {
            OneHotUtils.getOneHotCode(3, 0);
        }
        if (sex.equals("???")) {
            OneHotUtils.getOneHotCode(3, 1);
        }
        return OneHotUtils.getOneHotCode(3, 2);
    }

    private static String sun(String sun) {
        if (sun == null
                || sun.isEmpty()
        ) {
            return null;
        }
        return OneHotUtils.getOneHotCode(ConstellationEnums.getTotalConstellation(), ConstellationEnums.getIndexByConstellation(sun));
    }

    private static String vip(Integer isVip) {
        if (isVip == null) {
            return null;
        }
        return OneHotUtils.getOneHotCode(2, isVip);
    }

    private static String city(String city) {
        if (city == null) {
            return null;
        }
        if (cityMap.containsKey(city)) {
            return OneHotUtils.getOneHotCode(8, Integer.parseInt(cityMap.get(city)));
        }
        for (Map.Entry <String, String> entry : cityMap.entrySet()) {
            if (entry.getKey().contains(city)) {
                return OneHotUtils.getOneHotCode(8, Integer.parseInt(cityMap.get(entry.getKey())));
            }
        }
        return OneHotUtils.getOneHotCode(8, 7);
    }

    private static String manufacturer(String manufacturer) {
        if (manufacturer == null
                || manufacturer.isEmpty()
        ) {
            return null;
        }
        return OneHotUtils.getOneHotCode(BrandEnums.getTotalBrands(), BrandEnums.getIndexByBrand(manufacturer));
    }

    private static String os(String os) {
        if (os == null) {
            return null;
        }
        if ("IOS".equals(os.toUpperCase())) {
            return OneHotUtils.getOneHotCode(2, 0);
        }
        return OneHotUtils.getOneHotCode(2, 1);
    }

    private static String networkType(String netType) {
        if (netType == null) {
            return null;
        }
        if (netType.equals("2G")) {
            return OneHotUtils.getOneHotCode(6, 0);
        }
        if (netType.equals("3G")) {
            return OneHotUtils.getOneHotCode(6, 1);
        }
        if (netType.equals("4G")) {
            return OneHotUtils.getOneHotCode(6, 2);
        }
        if (netType.equals("5G")) {
            return OneHotUtils.getOneHotCode(6, 3);
        }
        if (netType.toUpperCase().equals("WIFI")) {
            return OneHotUtils.getOneHotCode(6, 4);
        }
        return OneHotUtils.getOneHotCode(6, 5);
    }

    private static String carrier(String carrier) {
        if (carrier == null) {
            return null;
        }
        if (carrier.equals("????????????")) {
            return OneHotUtils.getOneHotCode(4, 0);
        }
        if (carrier.equals("????????????")) {
            return OneHotUtils.getOneHotCode(4, 1);
        }
        if (carrier.equals("????????????")) {
            return OneHotUtils.getOneHotCode(4, 2);
        }
        return OneHotUtils.getOneHotCode(4, 3);
    }

    private static String isStaff(Integer staff) {
        if (staff == null) {
            return null;
        }
        if (staff == 1) {
            return OneHotUtils.getOneHotCode(2, 1);
        }
        return OneHotUtils.getOneHotCode(2, 0);
    }

    private static String pay(String pay) {
        if (pay == null || pay.isEmpty()) {
            return null;
        }
        double payDouble = Double.parseDouble(pay);
        if (payDouble == 0) {
            return OneHotUtils.getOneHotCode(6, 0);
        }
        if (payDouble <= 10) {
            return OneHotUtils.getOneHotCode(6, 1);
        }
        if (payDouble <= 100) {
            return OneHotUtils.getOneHotCode(6, 2);
        }
        if (payDouble <= 1000) {
            return OneHotUtils.getOneHotCode(6, 3);
        }
        if (payDouble <= 10000) {
            return OneHotUtils.getOneHotCode(6, 4);
        }
        return OneHotUtils.getOneHotCode(6, 5);
    }

    private static String screenHeight(String height) {
        if (height == null) {
            return null;
        }
        BigDecimal maxHeight = new BigDecimal(3840);
        BigDecimal minHeight = new BigDecimal(200);
        BigDecimal bigDecimal = new BigDecimal(height);
        BigDecimal normalized = normalized(bigDecimal, minHeight, maxHeight, 10);
        return normalized.toPlainString();
    }

    private static String screenWidth(String width) {
        BigDecimal maxWidth = new BigDecimal(3100);
        BigDecimal minWidth = new BigDecimal(200);
        if (width == null) {
            return null;
        }
        BigDecimal bigDecimal = new BigDecimal(width);
        BigDecimal normalized = normalized(bigDecimal, minWidth, maxWidth, 10);
        return normalized.toPlainString();
    }

    private static BigDecimal normalized(BigDecimal x, BigDecimal min, BigDecimal max, int digits) {
        return x.subtract(min).divide(max.subtract(min), digits , BigDecimal.ROUND_HALF_UP);
    }
}

