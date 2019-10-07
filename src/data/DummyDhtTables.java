package data;

public enum DummyDhtTables {
    STAT_INFO ("statinfo"),
    LOAD_INFO("loadinfo"),
    HISTORICAL_LOAD_INFO("historicalloadinfo"),
    MIGRATE_INFO("migrate");

    private final String name;

    DummyDhtTables(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getNameWithPrefix(String mode, String scheme) {
        return mode + "_" + scheme + "_" + name;
    }
}
