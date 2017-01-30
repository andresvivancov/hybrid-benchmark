package de.tuberlin.serialization;

import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder$;
import kafka.utils.VerifiableProperties;
import scala.reflect.ScalaSignature;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by patrick on 02.01.17.
 */
@ScalaSignature(
        bytes = "\u0006\u0001E3A!\u0001\u0002\u0001\u000f\ti1\u000b\u001e:j]\u001e$UmY8eKJT!a\u0001\u0003\u0002\u0015M,\'/[1mSj,\'OC\u0001\u0006\u0003\u0015Y\u0017MZ6b\u0007\u0001\u00192\u0001\u0001\u0005\u000f!\tIA\"D\u0001\u000b\u0015\u0005Y\u0011!B:dC2\f\u0017BA\u0007\u000b\u0005\u0019\te.\u001f*fMB\u0019q\u0002\u0005\n\u000e\u0003\tI!!\u0005\u0002\u0003\u000f\u0011+7m\u001c3feB\u00111C\u0006\b\u0003\u0013QI!!\u0006\u0006\u0002\rA\u0013X\rZ3g\u0013\t9\u0002D\u0001\u0004TiJLgn\u001a\u0006\u0003+)A\u0001B\u0007\u0001\u0003\u0002\u0003\u0006IaG\u0001\u0006aJ|\u0007o\u001d\t\u00039}i\u0011!\b\u0006\u0003=\u0011\tQ!\u001e;jYNL!\u0001I\u000f\u0003)Y+\'/\u001b4jC\ndW\r\u0015:pa\u0016\u0014H/[3t\u0011\u0015\u0011\u0003\u0001\"\u0001$\u0003\u0019a\u0014N\\5u}Q\u0011A%\n\t\u0003\u001f\u0001AqAG\u0011\u0011\u0002\u0003\u00071\u0004C\u0004(\u0001\t\u0007I\u0011\u0001\u0015\u0002\u0011\u0015t7m\u001c3j]\u001e,\u0012!\u000b\t\u0003U=j\u0011a\u000b\u0006\u0003Y5\nA\u0001\\1oO*\ta&\u0001\u0003kCZ\f\u0017BA\f,\u0011\u0019\t\u0004\u0001)A\u0005S\u0005IQM\\2pI&tw\r\t\u0005\u0006g\u0001!\t\u0001N\u0001\nMJ|WNQ=uKN$\"AE\u001b\t\u000bY\u0012\u0004\u0019A\u001c\u0002\u000b\tLH/Z:\u0011\u0007%A$(\u0003\u0002:\u0015\t)\u0011I\u001d:bsB\u0011\u0011bO\u0005\u0003y)\u0011AAQ=uK\u001e9aHAA\u0001\u0012\u0003y\u0014!D*ue&tw\rR3d_\u0012,\'\u000f\u0005\u0002\u0010\u0001\u001a9\u0011AAA\u0001\u0012\u0003\t5C\u0001!\t\u0011\u0015\u0011\u0003\t\"\u0001D)\u0005y\u0004bB#A#\u0003%\tAR\u0001\u001cI1,7o]5oSR$sM]3bi\u0016\u0014H\u0005Z3gCVdG\u000fJ\u0019\u0016\u0003\u001dS#a\u0007%,\u0003%\u0003\"AS(\u000e\u0003-S!\u0001T\'\u0002\u0013Ut7\r[3dW\u0016$\'B\u0001(\u000b\u0003)\tgN\\8uCRLwN\\\u0005\u0003!.\u0013\u0011#\u001e8dQ\u0016\u001c7.\u001a3WCJL\u0017M\\2f\u0001"
)
public class SparkStringTsDecoder implements Decoder<String> {

        private final String encoding;

        public static VerifiableProperties $lessinit$greater$default$1() {
            return StringDecoder$.MODULE$.$lessinit$greater$default$1();
        }

        public String encoding() {
            return this.encoding;
        }

        public String fromBytes(byte[] bytes) {
            String message="";
            try {
                message = new String(bytes, this.encoding()+System.currentTimeMillis());
            }catch(UnsupportedEncodingException e){
                e.printStackTrace();
            }
            return message;
        }

        public SparkStringTsDecoder(VerifiableProperties props) {
            this.encoding = props == null?"UTF8":props.getString("serializer.encoding", "UTF8");
        }

}
