package streamingRetention.usecases.linearRoad;

import util.BaseTuple;

import java.util.Objects;

public class MultiKeyLR {
    private int seg = -1;
    private int dir = -1;
    private long vid = -1;

    public MultiKeyLR(int seg, int dir) {
        this.seg = seg;
        this.dir = dir;
    }

    public MultiKeyLR(int seg, int dir, long vid) {
        this(seg, dir);
        this.vid = vid;
    }

    public static MultiKeyLR of(int seg, int dir) {
        return new MultiKeyLR(seg, dir);
    }

    public static MultiKeyLR of(int seg, int dir, long vid) {
        return new MultiKeyLR(seg, dir, vid);
    }

    public static MultiKeyLR segDirVid(LinearRoadInputTuple t ) {
        return new MultiKeyLR(t.getSeg(), t.getDir(), t.getVid() );
    }

    public static MultiKeyLR segDir(BaseTuple t) {
        String[] keystringSplit = t.getKey().split(":");
        return new MultiKeyLR(
                Integer.parseInt(keystringSplit[0]),
                Integer.parseInt(keystringSplit[1]));
    }

    public int getSeg() {
        return seg;
    }

    public int getDir() {
        return dir;
    }

    public long getVid() {
        return vid;
    }

    @Override
    public String toString() {
        return seg+":"+dir+":"+vid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiKeyLR multiKey = (MultiKeyLR) o;
        return seg == multiKey.seg && dir == multiKey.dir && vid == multiKey.vid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(seg, dir, vid);
    }
}
