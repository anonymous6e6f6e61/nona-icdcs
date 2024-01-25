package streamingRetention.usecases.carLocal;
import java.io.Serializable;

public class T4<T0, T1, T2, T3> implements Serializable {
    public T0 f0;
    public T1 f1;
    public T2 f2;
    public T3 f3;

    public T4(T0 f0, T1 f1, T2 f2, T3 f3) {
        this.f0 = f0;
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
    }

    public T4() {
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
}
