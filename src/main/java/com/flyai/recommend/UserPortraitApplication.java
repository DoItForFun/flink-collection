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
        cityMap.put("陇南市", "5");
        cityMap.put("云浮市", "5");
        cityMap.put("德州市", "4");
        cityMap.put("双鸭山市", "5");
        cityMap.put("莆田市", "3");
        cityMap.put("赣州市", "3");
        cityMap.put("重庆市", "1");
        cityMap.put("保定市", "3");
        cityMap.put("上海市", "0");
        cityMap.put("晋城市", "5");
        cityMap.put("博尔塔拉蒙古自治州", "5");
        cityMap.put("武威市", "5");
        cityMap.put("潍坊市", "3");
        cityMap.put("崇左市", "5");
        cityMap.put("沈阳市", "1");
        cityMap.put("漳州市", "3");
        cityMap.put("通化市", "5");
        cityMap.put("泰州市", "3");
        cityMap.put("山南市", "5");
        cityMap.put("牡丹江市", "4");
        cityMap.put("通辽市", "5");
        cityMap.put("连云港市", "3");
        cityMap.put("吕梁市", "5");
        cityMap.put("镇江市", "3");
        cityMap.put("厦门市", "2");
        cityMap.put("鹤壁市", "5");
        cityMap.put("荆州市", "3");
        cityMap.put("南充市", "4");
        cityMap.put("延边朝鲜族自治州", "4");
        cityMap.put("雅安市", "5");
        cityMap.put("清远市", "3");
        cityMap.put("开封市", "4");
        cityMap.put("阜新市", "5");
        cityMap.put("鹤岗市", "5");
        cityMap.put("绵阳市", "3");
        cityMap.put("武汉市", "1");
        cityMap.put("盐城市", "3");
        cityMap.put("南阳市", "3");
        cityMap.put("巴中市", "5");
        cityMap.put("兰州市", "2");
        cityMap.put("巴彦淖尔市", "5");
        cityMap.put("潮州市", "3");
        cityMap.put("宜宾市", "4");
        cityMap.put("锡林郭勒盟", "5");
        cityMap.put("济宁市", "3");
        cityMap.put("宜春市", "4");
        cityMap.put("朝阳市", "5");
        cityMap.put("烟台市", "2");
        cityMap.put("丽水市", "3");
        cityMap.put("温州市", "2");
        cityMap.put("伊春市", "5");
        cityMap.put("洛阳市", "3");
        cityMap.put("遵义市", "3");
        cityMap.put("湛江市", "3");
        cityMap.put("滨州市", "4");
        cityMap.put("大兴安岭地区", "5");
        cityMap.put("中卫市", "5");
        cityMap.put("韶关市", "4");
        cityMap.put("鸡西市", "5");
        cityMap.put("惠州市", "2");
        cityMap.put("太原市", "2");
        cityMap.put("抚顺市", "5");
        cityMap.put("北京市", "0");
        cityMap.put("龙岩市", "3");
        cityMap.put("伊犁哈萨克自治州", "5");
        cityMap.put("揭阳市", "3");
        cityMap.put("包头市", "3");
        cityMap.put("常德市", "4");
        cityMap.put("银川市", "3");
        cityMap.put("朔州市", "5");
        cityMap.put("娄底市", "4");
        cityMap.put("铁岭市", "5");
        cityMap.put("苏州市", "1");
        cityMap.put("郑州市", "1");
        cityMap.put("乌海市", "5");
        cityMap.put("来宾市", "5");
        cityMap.put("三亚市", "3");
        cityMap.put("延安市", "5");
        cityMap.put("六安市", "4");
        cityMap.put("焦作市", "4");
        cityMap.put("克孜勒苏柯尔克孜自治州", "5");
        cityMap.put("宜昌市", "3");
        cityMap.put("菏泽市", "4");
        cityMap.put("德阳市", "4");
        cityMap.put("德宏傣族景颇族自治州", "4");
        cityMap.put("张家界市", "5");
        cityMap.put("台州市", "2");
        cityMap.put("南京市", "1");
        cityMap.put("拉萨市", "4");
        cityMap.put("上饶市", "3");
        cityMap.put("固原市", "5");
        cityMap.put("长沙市", "1");
        cityMap.put("防城港市", "5");
        cityMap.put("河源市", "4");
        cityMap.put("承德市", "4");
        cityMap.put("黄冈市", "4");
        cityMap.put("石家庄市", "2");
        cityMap.put("商洛市", "5");
        cityMap.put("安顺市", "4");
        cityMap.put("运城市", "4");
        cityMap.put("南昌市", "2");
        cityMap.put("邯郸市", "3");
        cityMap.put("河池市", "5");
        cityMap.put("中山市", "2");
        cityMap.put("黄石市", "4");
        cityMap.put("鄂州市", "5");
        cityMap.put("鹰潭市", "4");
        cityMap.put("白银市", "5");
        cityMap.put("金昌市", "5");
        cityMap.put("黑河市", "5");
        cityMap.put("怒江傈僳族自治州", "5");
        cityMap.put("宁德市", "3");
        cityMap.put("四平市", "5");
        cityMap.put("金华市", "2");
        cityMap.put("安庆市", "3");
        cityMap.put("常州市", "2");
        cityMap.put("湖州市", "3");
        cityMap.put("濮阳市", "5");
        cityMap.put("新乡市", "3");
        cityMap.put("安康市", "5");
        cityMap.put("孝感市", "4");
        cityMap.put("临沧市", "5");
        cityMap.put("茂名市", "4");
        cityMap.put("周口市", "4");
        cityMap.put("甘孜藏族自治州", "5");
        cityMap.put("黔南布依族苗族自治州", "4");
        cityMap.put("日照市", "4");
        cityMap.put("临汾市", "4");
        cityMap.put("绥化市", "4");
        cityMap.put("聊城市", "4");
        cityMap.put("青岛市", "1");
        cityMap.put("玉林市", "4");
        cityMap.put("台湾省", "6");
        cityMap.put("阳江市", "4");
        cityMap.put("泉州市", "2");
        cityMap.put("绍兴市", "2");
        cityMap.put("海西蒙古族藏族自治州", "5");
        cityMap.put("喀什地区", "5");
        cityMap.put("咸宁市", "4");
        cityMap.put("临夏回族自治州", "5");
        cityMap.put("迪庆藏族自治州", "5");
        cityMap.put("永州市", "4");
        cityMap.put("儋州市", "5");
        cityMap.put("芜湖市", "3");
        cityMap.put("六盘水市", "4");
        cityMap.put("滁州市", "3");
        cityMap.put("西双版纳傣族自治州", "4");
        cityMap.put("哈尔滨市", "2");
        cityMap.put("齐齐哈尔市", "4");
        cityMap.put("呼和浩特市", "3");
        cityMap.put("天水市", "5");
        cityMap.put("张掖市", "5");
        cityMap.put("衡阳市", "3");
        cityMap.put("酒泉市", "5");
        cityMap.put("长治市", "5");
        cityMap.put("忻州市", "5");
        cityMap.put("无锡市", "2");
        cityMap.put("枣庄市", "4");
        cityMap.put("徐州市", "2");
        cityMap.put("阿克苏地区", "5");
        cityMap.put("内江市", "5");
        cityMap.put("渭南市", "4");
        cityMap.put("定西市", "5");
        cityMap.put("阜阳市", "3");
        cityMap.put("赤峰市", "4");
        cityMap.put("林芝市", "5");
        cityMap.put("桂林市", "3");
        cityMap.put("梧州市", "4");
        cityMap.put("廊坊市", "3");
        cityMap.put("马鞍山市", "3");
        cityMap.put("蚌埠市", "3");
        cityMap.put("松原市", "5");
        cityMap.put("池州市", "5");
        cityMap.put("葫芦岛市", "5");
        cityMap.put("澳门", "6");
        cityMap.put("淄博市", "3");
        cityMap.put("吉林市", "3");
        cityMap.put("阿勒泰地区", "5");
        cityMap.put("株洲市", "3");
        cityMap.put("海东市", "5");
        cityMap.put("信阳市", "3");
        cityMap.put("铜仁市", "4");
        cityMap.put("肇庆市", "3");
        cityMap.put("抚州市", "4");
        cityMap.put("贺州市", "5");
        cityMap.put("泰安市", "3");
        cityMap.put("杭州市", "1");
        cityMap.put("黄山市", "4");
        cityMap.put("佛山市", "2");
        cityMap.put("呼伦贝尔市", "5");
        cityMap.put("岳阳市", "3");
        cityMap.put("白城市", "5");
        cityMap.put("楚雄彝族自治州", "5");
        cityMap.put("贵港市", "5");
        cityMap.put("保山市", "4");
        cityMap.put("巴音郭楞蒙古自治州", "5");
        cityMap.put("西宁市", "4");
        cityMap.put("淮安市", "3");
        cityMap.put("七台河市", "5");
        cityMap.put("南宁市", "2");
        cityMap.put("海口市", "2");
        cityMap.put("西安市", "1");
        cityMap.put("克拉玛依市", "5");
        cityMap.put("鞍山市", "3");
        cityMap.put("淮北市", "5");
        cityMap.put("阳泉市", "5");
        cityMap.put("自贡市", "5");
        cityMap.put("阿里地区", "5");
        cityMap.put("白山市", "5");
        cityMap.put("广元市", "5");
        cityMap.put("铜川市", "5");
        cityMap.put("吴忠市", "5");
        cityMap.put("汕头市", "3");
        cityMap.put("许昌市", "4");
        cityMap.put("泸州市", "4");
        cityMap.put("张家口市", "4");
        cityMap.put("广州市", "0");
        cityMap.put("宿州市", "4");
        cityMap.put("日喀则市", "5");
        cityMap.put("佳木斯市", "4");
        cityMap.put("玉树藏族自治州", "5");
        cityMap.put("郴州市", "3");
        cityMap.put("商丘市", "3");
        cityMap.put("宝鸡市", "4");
        cityMap.put("汕尾市", "4");
        cityMap.put("秦皇岛市", "3");
        cityMap.put("随州市", "5");
        cityMap.put("攀枝花", "5");
        cityMap.put("深圳市", "0");
        cityMap.put("南平市", "3");
        cityMap.put("普洱市", "5");
        cityMap.put("唐山市", "3");
        cityMap.put("营口市", "4");
        cityMap.put("吉安市", "4");
        cityMap.put("毕节市", "4");
        cityMap.put("丹东市", "4");
        cityMap.put("亳州市", "4");
        cityMap.put("那曲市", "5");
        cityMap.put("东莞市", "1");
        cityMap.put("宿迁市", "3");
        cityMap.put("舟山市", "3");
        cityMap.put("阿坝藏族羌族自治州", "5");
        cityMap.put("萍乡市", "5");
        cityMap.put("阿拉善盟", "5");
        cityMap.put("资阳市", "5");
        cityMap.put("珠海市", "2");
        cityMap.put("眉山市", "4");
        cityMap.put("辽阳市", "5");
        cityMap.put("黄南藏族自治州", "5");
        cityMap.put("淮南市", "4");
        cityMap.put("盘锦市", "4");
        cityMap.put("本溪市", "5");
        cityMap.put("嘉峪关市", "5");
        cityMap.put("邢台市", "4");
        cityMap.put("榆林市", "4");
        cityMap.put("嘉兴市", "2");
        cityMap.put("安阳市", "4");
        cityMap.put("沧州市", "3");
        cityMap.put("衢州市", "4");
        cityMap.put("福州市", "2");
        cityMap.put("昆明市", "1");
        cityMap.put("九江市", "3");
        cityMap.put("三明市", "3");
        cityMap.put("柳州市", "3");
        cityMap.put("百色市", "4");
        cityMap.put("黔西南布依族苗族自治州", "5");
        cityMap.put("北海市", "4");
        cityMap.put("石嘴山市", "5");
        cityMap.put("南通市", "2");
        cityMap.put("玉溪市", "5");
        cityMap.put("铜陵市", "4");
        cityMap.put("恩施土家族苗族自治州", "4");
        cityMap.put("遂宁市", "5");
        cityMap.put("天津市", "1");
        cityMap.put("宣城市", "4");
        cityMap.put("东营市", "4");
        cityMap.put("果洛藏族自治州", "5");
        cityMap.put("香港", "6");
        cityMap.put("文山壮族苗族自治州", "5");
        cityMap.put("咸阳市", "3");
        cityMap.put("甘南藏族自治州", "5");
        cityMap.put("凉山彝族自治州", "5");
        cityMap.put("乌兰察布市", "5");
        cityMap.put("大理市", "4");
        cityMap.put("新余市", "5");
        cityMap.put("钦州市", "5");
        cityMap.put("乌鲁木齐市", "3");
        cityMap.put("大同市", "4");
        cityMap.put("鄂尔多斯市", "4");
        cityMap.put("黔东南苗族侗族自治州", "4");
        cityMap.put("荆门市", "5");
        cityMap.put("成都市", "1");
        cityMap.put("海北藏族自治州", "5");
        cityMap.put("平凉市", "5");
        cityMap.put("襄阳市", "3");
        cityMap.put("昭通市", "5");
        cityMap.put("庆阳市", "5");
        cityMap.put("乐山市", "4");
        cityMap.put("达州市", "5");
        cityMap.put("平顶山市", "4");
        cityMap.put("邵阳市", "4");
        cityMap.put("曲靖市", "4");
        cityMap.put("塔城地区", "5");
        cityMap.put("三沙市", "5");
        cityMap.put("红河哈尼族彝族自治州", "4");
        cityMap.put("衡水市", "5");
        cityMap.put("江门市", "3");
        cityMap.put("晋中市", "4");
        cityMap.put("哈密市", "5");
        cityMap.put("汉中市", "5");
        cityMap.put("兴安盟", "5");
        cityMap.put("吐鲁番市", "5");
        cityMap.put("大连市", "2");
        cityMap.put("和田地区", "5");
        cityMap.put("大庆市", "3");
        cityMap.put("丽江市", "4");
        cityMap.put("辽源市", "5");
        cityMap.put("长春市", "2");
        cityMap.put("宁波市", "1");
        cityMap.put("济南市", "2");
        cityMap.put("广安市", "5");
        cityMap.put("十堰市", "4");
        cityMap.put("昌都市", "5");
        cityMap.put("临沂市", "3");
        cityMap.put("怀化市", "4");
        cityMap.put("合肥市", "2");
        cityMap.put("昌吉回族自治州", "5");
        cityMap.put("景德镇市", "4");
        cityMap.put("湘潭市", "3");
        cityMap.put("海南藏族自治州", "5");
        cityMap.put("驻马店市", "4");
        cityMap.put("漯河市", "5");
        cityMap.put("锦州市", "4");
        cityMap.put("贵阳市", "2");
        cityMap.put("湘西土家族苗族自治州", "5");
        cityMap.put("梅州市", "3");
        cityMap.put("益阳市", "4");
        cityMap.put("威海市", "3");
        cityMap.put("三门峡市", "5");
        cityMap.put("扬州市", "2");
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
                                        && (properties.get("node_name").equals("开播")
                                        || properties.get("node_name").equals("抢答")
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
        // 保存用户数据
        baseStream.addSink(new MyTiDbSink <>());
        // 保存ip池数据
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
        // 保存用户向量数据
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
        // @todo 年龄
        if (userEntity.getBirthday() != null) {
            String ageVector = age(userEntity.getBirthday());
            if (ageVector != null) {
                userVectorEntity.setBirthday_vector(ageVector);
            }
        }
        // @todo 性别向量
        if (userEntity.getSex() != null) {
            String sexVector = sex(userEntity.getSex());
            if (sexVector != null) {
                userVectorEntity.setSex_vector(sexVector);
            }
        }
        // @todo 太阳星座向量
        if (userEntity.getSun() != null) {
            String sunVector = sun(userEntity.getSun());
            if (sunVector != null) {
                userVectorEntity.setSun_vector(sunVector);
            }
        }
        // @todo 会员向量
        if (userEntity.getIs_vip() != null) {
            String vipVector = vip(userEntity.getIs_vip());
            if (vipVector != null) {
                userVectorEntity.setIs_vip_vector(vipVector);
            }
        }
        // @todo 注册时间星期向量
        if (userEntity.getRegister_time() != null) {
            String registerTimeVector = VectorUtils.week(String.valueOf(userEntity.getRegister_time()));
            if (registerTimeVector != null) {
                userVectorEntity.setRegister_time_vector(registerTimeVector);
            }
        }
        // @todo 当前时间星期向量
        userVectorEntity.setAction_time_vector(VectorUtils.week(String.valueOf(new Date().getTime())));
        // @todo 几线城市向量
        if (userEntity.getCity() != null) {
            String cityVector = city(userEntity.getCity());
            if (cityVector != null) {
                userVectorEntity.setCity_vector(cityVector);
            }
        }
        // @todo 手机品牌向量
        if (userEntity.getManufacturer() != null) {
            String manufacturerVector = manufacturer(userEntity.getManufacturer());
            if (manufacturerVector != null) {
                userVectorEntity.setManufacturer_vector(manufacturerVector);
            }
        }
        // @todo 手机系统向量
        if (userEntity.getOs() != null) {
            String osVector = os(userEntity.getOs());
            if (osVector != null) {
                userVectorEntity.setOs_vector(osVector);
            }
        }
        // @todo 屏幕长度minmax
        if (userEntity.getScreen_height() != null) {
            String screenHeightMinMax = screenHeight(userEntity.getScreen_height());
            if (screenHeightMinMax != null) {
                userVectorEntity.setScreen_height_vector(screenHeightMinMax);
            }
        }
        // @todo 屏幕宽度minmax
        if (userEntity.getScreen_width() != null) {
            String screenWidthMinMax = screenWidth(userEntity.getScreen_width());
            if (screenWidthMinMax != null) {
                userVectorEntity.setScreen_width_vector(screenWidthMinMax);
            }
        }
        // @todo 网络类型向量
        if (userEntity.getNetwork_type() != null) {
            String networkTypeVector = networkType(userEntity.getNetwork_type());
            if (networkTypeVector != null) {
                userVectorEntity.setNetwork_type_vector(networkTypeVector);
            }
        }
        // @todo 供应商向量
        if (userEntity.getCarrier() != null) {
            String carrierVector = carrier(userEntity.getCarrier());
            if (carrierVector != null) {
                userVectorEntity.setCarrier_vector(carrierVector);
            }
        }
        // @todo 是否达人向量
        if (userEntity.getStaff() != null) {
            String staffVector = isStaff(userEntity.getStaff());
            if (staffVector != null) {
                userVectorEntity.setStaff_vector(staffVector);
            }
        }
        // @todo 充值金额向量
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

        if (sex.equals("男")) {
            OneHotUtils.getOneHotCode(3, 0);
        }
        if (sex.equals("女")) {
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
        if (carrier.equals("中国移动")) {
            return OneHotUtils.getOneHotCode(4, 0);
        }
        if (carrier.equals("中国联通")) {
            return OneHotUtils.getOneHotCode(4, 1);
        }
        if (carrier.equals("中国电信")) {
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

