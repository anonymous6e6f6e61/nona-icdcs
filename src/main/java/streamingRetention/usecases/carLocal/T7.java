package streamingRetention.usecases.carLocal;
import java.io.Serializable;

public class T7<T0, T1, T2, T3, T4, T5, T6> implements Serializable {
    public T0 f0;
    public T1 f1;
    public T2 f2;
    public T3 f3;
    public T4 f4;
    public T5 f5;
    public T6 f6;

    public T7(T0 f0, T1 f1, T2 f2, T3 f3, T4 f4, T5 f5, T6 f6) {
        this.f0 = f0;
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.f5 = f5;
        this.f6 = f6;
    }

    public T7() {
    }

    public static <T0, T1, T2, T3, T4, T5, T6> T7<T0, T1, T2, T3, T4, T5, T6> of(
            T0 value0, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6) {
        return new T7<>(value0, value1, value2, value3, value4, value5, value6);
    }

    public T0 getF0() {
        return f0;
    }

    public void setF0(T0 f0) {
        this.f0 = f0;
    }

    public T1 getF1() {
        return f1;
    }

    public void setF1(T1 f1) {
        this.f1 = f1;
    }

    public T2 getF2() {
        return f2;
    }

    public void setF2(T2 f2) {
        this.f2 = f2;
    }

    public T3 getF3() {
        return f3;
    }

    public void setF3(T3 f3) {
        this.f3 = f3;
    }

    public T4 getF4() {
        return f4;
    }

    public void setF4(T4 f4) {
        this.f4 = f4;
    }

    public T5 getF5() {
        return f5;
    }

    public void setF5(T5 f5) {
        this.f5 = f5;
    }

    public T6 getF6() {
        return f6;
    }

    public void setF6(T6 f6) {
        this.f6 = f6;
    }
}
