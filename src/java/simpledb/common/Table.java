package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.TupleDesc;

public class Table {
    private int id;
    private DbFile file;
    private String name;
    private String pkeyField;

    private TupleDesc td;

    public Table(int id, DbFile file, String name, String pkeyField) {
        this.id = id;
        this.file = file;
        this.name = name;
        this.pkeyField = pkeyField;
    }
    
    public DbFile getFile() {
        return file;
    }
    public void setFile(DbFile file) {
        this.file = file;
    }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public String getPkeyField() {
        return pkeyField;
    }
    public void setPkeyField(String pkeyField) {
        this.pkeyField = pkeyField;
    }
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public TupleDesc getTd() {
        return td;
    }
    public void setTd(TupleDesc td) {
        this.td = td;
    }
    
}
