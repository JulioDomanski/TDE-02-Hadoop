package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HLAWritable implements WritableComparable<HLAWritable> {
    private int year;
    private String country;

    // Construtor padrão
    public HLAWritable() {}

    // Construtor com parâmetros
    public HLAWritable(int year, String country) {
        this.year = year;
        this.country = country;
    }

    // Getters e setters
    public int getYear() {
        return year;
    }

    public String getCountry() {
        return country;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public String toString() {
        return year + "\t" + country;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);  // Escreve o ano como inteiro
        dataOutput.writeUTF(country);  // Escreve o país como string
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();  // Lê o ano como inteiro
        country = dataInput.readUTF();  // Lê o país como string
    }

    @Override
    public int compareTo(HLAWritable o) {
        int cmp = Integer.compare(year, o.year);
        if (cmp != 0){
            return cmp;
        }
        return country.compareTo(o.country);
    }
}