package commonmodels;

public interface Terminal {
    void initialize();
    void destroy();
    void printInfo();
    void execute(String[] args);
}
