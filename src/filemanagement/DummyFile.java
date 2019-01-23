package filemanagement;

public class DummyFile {

    private String name;

    private long size;

    public DummyFile() {
        this.size = -1;
    }

    public DummyFile(String file) {
        this();
        String[] temp = file.split(" ");
        this.name = temp[0];

        if (temp.length == 2) {
            this.size = Long.valueOf(temp[1]);
        }
    }

    public DummyFile(String name, long size) {
        this.name = name;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String toAttachment() {
        return name + " " + size;
    }
}
