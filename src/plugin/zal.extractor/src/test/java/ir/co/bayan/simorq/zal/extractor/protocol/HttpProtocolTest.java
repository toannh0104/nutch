package ir.co.bayan.simorq.zal.extractor.protocol;

import static org.junit.Assert.*;
import ir.co.bayan.simorq.zal.extractor.core.Content;
import ir.co.bayan.simorq.zal.extractor.protocol.DirectHttpProtocol;
import ir.co.bayan.simorq.zal.extractor.protocol.ProtocolException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Taha Ghasemi <taha.ghasemi@gmail.com>
 * 
 */
public class HttpProtocolTest {

	/**
	 * Test method for
	 * {@link ir.co.bayan.simorq.zal.extractor.protocol.DirectHttpProtocol#fetch(java.net.URL, java.util.Map)}.
	 * 
	 * @throws ProtocolException
	 * @throws IOException 
	 */
	@Test
//	@Ignore
	public void testFetch() throws ProtocolException, IOException {
		DirectHttpProtocol protocol = new DirectHttpProtocol();
		Content content = protocol.fetch(new URL("http://www.doisongphapluat.com/xa-hoi/bua-tiec-mi-quang-doat-mang-co-con-dau-xau-so-a52048.html"), new HashMap<String, Object>());
		
		int c;
		for(int y = 0 ; y < 1; y++ ) {
	         while(( c= content.getData().read())!= -1) {
	            System.out.print(Character.toLowerCase((char)c));
	         }
	         content.getData().reset(); 
	      }
	   
		
		assertEquals("text/html", content.getType());
		assertEquals("ISO-8859-1", content.getEncoding());
	}

}
