package kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.FieldSerializer
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by pengxingxiong@ruijie.com.cn on 2019/7/2 11:37
  */
class ToKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Person], new FieldSerializer(kryo, classOf[Person])) //在Kryo序列化库中注册自定义的类

  }
}
