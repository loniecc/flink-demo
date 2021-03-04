package com.example.demo.demo01;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author huzeming@sensorsdata.com
 * @version 1.0.0
 * @since 2021/03/04 16:24
 */
public class PersonFilter {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Person> originPersonList = environment.fromElements(
        new Person("aaaa", 13),
        new Person("bbbb", 18),
        new Person("cccc", 20)
    );

    DataStream<Person> adult = originPersonList.filter((person -> person.getAge() > 15));

    adult.print();

    environment.execute("年龄过滤");
  }


  public static class Person {
    int age;
    String name;

    public Person() {
    }

    public Person(String name, int age) {
      this.age = age;
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }


    @Override
    public String toString() {
      return "Person{" +
          "age=" + age +
          ", name='" + name + '\'' +
          '}';
    }
  }
}
