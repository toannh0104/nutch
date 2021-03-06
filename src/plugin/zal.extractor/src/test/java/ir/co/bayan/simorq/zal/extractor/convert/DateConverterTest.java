package ir.co.bayan.simorq.zal.extractor.convert;

import static org.junit.Assert.assertTrue;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Taha Ghasemi <taha.ghasemi@gmail.com>
 * 
 */
public class DateConverterTest {

	private static DateConverter converter;

	@BeforeClass
	public static void init() {
		converter = new DateConverter();
	}

	@Test
	public void test() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(2013, 3 - 1, 9);
		assertTrue(DateUtils.isSameDay(calendar.getTime(), (Date) converter.convert("09/03/2013")));
	}

}
