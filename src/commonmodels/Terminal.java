package commonmodels;

public interface Terminal {
    void initialize();
    void destroy();
    void printInfo();
    String execute(String[] args);
}
