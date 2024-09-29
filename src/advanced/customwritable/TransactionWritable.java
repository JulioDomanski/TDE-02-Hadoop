package advanced.customwritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TransactionWritable implements WritableComparable<TransactionWritable> {
    private IntWritable year;
    private Text country;
    private DoubleWritable tradeValue;

    // Construtor padrão
    public TransactionWritable() {
        this.year = new IntWritable();
        this.country = new Text();
        this.tradeValue = new DoubleWritable();
    }

    // Construtor com parâmetros
    public TransactionWritable(int year, String country, double tradeValue) {
        this.year = new IntWritable(year);
        this.country = new Text(country);
        this.tradeValue = new DoubleWritable(tradeValue);
    }

    // Getters e setters
    public int getYear() {
        return year.get();
    }

    public String getCountry() {
        return country.toString();
    }

    public double getTradeValue() {
        return tradeValue.get();
    }

    public void setYear(int year) {
        this.year.set(year);
    }

    public void setCountry(String country) {
        this.country.set(country);
    }

    public void setTradeValue(double tradeValue) {
        this.tradeValue.set(tradeValue);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        year.write(dataOutput);
        country.write(dataOutput);
        tradeValue.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year.readFields(dataInput);
        country.readFields(dataInput);
        tradeValue.readFields(dataInput);
    }

    @Override
    public int compareTo(TransactionWritable o) {
        int cmp = Integer.compare(year.get(), o.year.get());
        if (cmp != 0) {
            return cmp;
        }
        cmp = country.compareTo(o.country);
        if (cmp != 0) {
            return cmp;
        }
        return Double.compare(tradeValue.get(), o.tradeValue.get());
    }

    @Override
    public String toString() {
        return year.toString() + "\t" + country.toString() + "\t" + tradeValue.toString();
    }
}