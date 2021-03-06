package ir.co.bayan.simorq.zal.extractor.model;

import ir.co.bayan.simorq.zal.extractor.evaluation.EvaluationContext;

import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.Validate;

/**
 * Truncates a string if its size is greater than max. In this case, it appends, the specified append string to the
 * result
 * 
 * @author Taha Ghasemi <taha.ghasemi@gmail.com>
 * 
 */
/**
 * @author Taha Ghasemi <taha.ghasemi@gmail.com>
 * 
 */
@XmlRootElement
public class Truncate extends Function {

	@XmlAttribute
	private int max;

	@XmlAttribute
	private String append;

	/**
	 * Indicates whether this function should only break the given item on whitespace characters
	 */
	@XmlAttribute
	private boolean breakOnWhitespaces = false;

	/**
	 * @return the max
	 */
	public int getMax() {
		return max;
	}

	/**
	 * @param max
	 *            the max to set
	 */
	public void setMax(int max) {
		this.max = max;
	}

	/**
	 * @return the append
	 */
	public String getAppend() {
		return append;
	}

	/**
	 * @param append
	 *            the append to set
	 */
	public void setAppend(String append) {
		this.append = append;
	}

	/**
	 * @return the breakOnWhitespaces
	 */
	public boolean isBreakOnWhitespaces() {
		return breakOnWhitespaces;
	}

	/**
	 * @param breakOnWhitespaces
	 *            the breakOnWhitespaces to set
	 */
	public void setBreakOnWhitespaces(boolean breakOnWhitespaces) {
		this.breakOnWhitespaces = breakOnWhitespaces;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<?> extract(Object root, EvaluationContext context) throws Exception {
		Validate.isTrue(args != null && args.size() == 1, "Only one inner function is expected.");
		List<String> res = (List<String>) args.get(0).extract(root, context);
		for (int i = 0; i < res.size(); i++) {
			String item = res.get(i);
			if (item != null && item.length() > max) {
				int last = max - 1;
				if (breakOnWhitespaces)
					while (last >= 0 && !Character.isWhitespace(item.charAt(last))) {
						last--;
					}
				item = item.substring(0, last + 1);
				if (append != null)
					item = item + append;
				res.set(i, item);
			}
		}
		return res;
	}

	@Override
	public String toString() {
		return "Truncate [max=" + max + "]";
	}

}
