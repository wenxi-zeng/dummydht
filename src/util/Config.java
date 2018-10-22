package util;

public class Config {

    public final static String CONFIG_RING = "ring-config";
    public final static String CONFIG_ELASTIC = "elastic-config";
    public final static String CONFIG_CEPH = "ceph-config";

    public final static String PROPERTY_HASH_SLOTS = "hash_slots";
    public final static String PROPERTY_START_IP = "start_ip";
    public final static String PROPERTY_IP_RANGE = "ip_range";
    public final static String PROPERTY_START_PORT = "start_port";
    public final static String PROPERTY_PORT_RANGE = "port_range";
    public final static String PROPERTY_NUMBER_OF_REPLICAS = "number_of_replicas";
    public final static String PROPERTY_NUMBER_OF_PHYSICAL_NODES = "number_of_physical_nodes";
    public final static String PROPERTY_VIRTUAL_PHYSICAL_RATIO = "virtual_physical_ratio";

    public static int NUMBER_OF_REPLICAS = 3;
    public static int NUMBER_OF_HASH_SLOTS = 100;
    public static int VIRTUAL_PHYSICAL_RATIO = 1;
}
