package glassPaxos.utils;

import glassPaxos.network.messages.Message;

import java.util.EnumMap;
import java.util.Iterator;

public class MessageProfiler {

    private final EnumMap<Message.MSG_TYPE, Long> msgTimingMap =
            new EnumMap<Message.MSG_TYPE, Long>(Message.MSG_TYPE.class);
    private long startTimestamp;     //start timestamp of program
    private long programTime;
    private long handleMsgTime;

    public enum FUNC_STAGE {
        S1, S2, S3, S4, S5
    }
    private final EnumMap<FUNC_STAGE, Long> funcTimingMap = new EnumMap<FUNC_STAGE, Long>(FUNC_STAGE.class);

    public MessageProfiler() {}

    public void initMsgTiming() {
        startTimestamp = System.currentTimeMillis();
        programTime = 0L;
        handleMsgTime = 0L;
        for (Message.MSG_TYPE type : Message.MSG_TYPE.values()) {
            msgTimingMap.put(type, 0L);
        }
        //System.out.println("initial msgTimingMap: " + msgTimingMap);
    }

    public void initFuncTiming() {
        for (FUNC_STAGE stage : FUNC_STAGE.values()) {
            funcTimingMap.put(stage, 0L);
        }
    }

    public void showMsgTiming() {
        if (/*progTime <= 0 ||*/ handleMsgTime <= 0) {
            System.out.printf("error handleMsgTime=%d\n", handleMsgTime);
            return;
        }

        Iterator<Message.MSG_TYPE> enumKeySet = msgTimingMap.keySet().iterator();
        double ratioH = 0.0;
        long msgTime = 0L;
        long expHandleTime = 0;
        System.out.printf("\n---- program=%.1f handleMsg=%.1f (s) ratio=%.2f\n", getProgramTime()/1000.0,
                handleMsgTime/1000000000.0, handleMsgTime/(getProgramTime()*1000000.0));
        System.out.printf("---- MSG_TYPE ---- ratio ----\n");
        while(enumKeySet.hasNext()){
            Message.MSG_TYPE curType = enumKeySet.next();
            msgTime = msgTimingMap.get(curType)/1000000;
            ratioH = msgTime*1000000.0/handleMsgTime;

            expHandleTime += msgTime;
            if (msgTime > 0.1) {
                System.out.printf("type=%s time=%.1f s ratio=%.2f\n", curType, msgTime / 1000.0, ratioH);
            }
        }
        System.out.printf("---- MSG_TYPE handleMsg=%.1f expected=%.1f\n\n", handleMsgTime/1000000000.0,
                expHandleTime/1000.0);
        //System.out.println("show msgTimingMap: " + msgTimingMap);
    }

    public void showFuncTiming(Message.MSG_TYPE mType) {
        double mTypeTotal = msgTimingMap.get(mType)/1000000000.0;
        System.out.printf("\nTIMING MSG_TYPE=%s total=%.1f s\n", mType.name(), mTypeTotal);
        Iterator<FUNC_STAGE> enumKeySet = funcTimingMap.keySet().iterator();
        double msgTime;
        while(enumKeySet.hasNext()) {
            FUNC_STAGE stage = enumKeySet.next();
            msgTime = funcTimingMap.get(stage)/1000000000.0;
            if (msgTime/mTypeTotal > 0.01) {
                System.out.printf("stage=%s time=%.1f s ratio=%.2f\n", stage, msgTime, msgTime / mTypeTotal);
            }
        }
        System.out.printf("---- MSG_TYPE=%s ----\n\n", mType.name());
    }

    public void incFuncTiming(FUNC_STAGE stage, long elapsedTime) {
        long old = funcTimingMap.get(stage);
        funcTimingMap.put(stage, old + elapsedTime);
    }

    public long getMsgTiming(Message.MSG_TYPE type) {
        return msgTimingMap.get(type);
    }

    public void setMsgTiming(Message.MSG_TYPE type, long time) {
        msgTimingMap.put(type, time);
    }

    public void incMsgTiming(Message.MSG_TYPE type, long time) {
        long old = msgTimingMap.get(type);
        msgTimingMap.put(type, old+time);
    }

    public void setProgramTime(long endTimestamp) {
        this.programTime = (endTimestamp - this.startTimestamp);
    }

    public long getProgramTime() {
        return this.programTime;
    }

    public void incHandleMsgTime(long time) {
        this.handleMsgTime = this.handleMsgTime + time;
    }

    public static void main(String []args) throws Exception {
        MessageProfiler profiler = new MessageProfiler();
        profiler.initMsgTiming();

        long execTime = 1000000000; //1s
        profiler.incMsgTiming(Message.MSG_TYPE.ACCEPT, execTime);
        profiler.setMsgTiming(Message.MSG_TYPE.SYNC, execTime/2);

        profiler.incHandleMsgTime(execTime * 3);
        Thread.sleep(execTime*5/1000000);
        profiler.setProgramTime(System.currentTimeMillis());

        profiler.showMsgTiming();

        profiler.initFuncTiming();
        profiler.incFuncTiming(FUNC_STAGE.S2, execTime/4);
        profiler.incFuncTiming(FUNC_STAGE.S4, execTime/2);
        profiler.showFuncTiming(Message.MSG_TYPE.ACCEPT);
    }
}
