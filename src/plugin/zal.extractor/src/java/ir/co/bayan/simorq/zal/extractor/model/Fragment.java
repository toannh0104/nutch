package ir.co.bayan.simorq.zal.extractor.model;

import ir.co.bayan.simorq.zal.extractor.core.ExtractedDoc;
import ir.co.bayan.simorq.zal.extractor.evaluation.EvaluationContext;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A fragment represents a subset of data in the document.
 * 
 * @author Taha Ghasemi <taha.ghasemi@gmail.com>
 * 
 */
public class Fragment extends Rooted {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(Fragment.class);

	public static final String TEXT_FIELD = "content";
	public static final String TITLE_FIELD = "title";
	public static final String URL_FIELD = "url";

	@XmlElement(name = "extract-to")
	private List<ExtractTo> extractTos = new ArrayList<ExtractTo>();

	@XmlElementWrapper(name = "outlinks")
	@XmlElementRef
	private List<Function> outlinks;

	public List<ExtractTo> getExtractTos() {
		return extractTos;
	}

	@Override
	public List<ExtractedDoc> extract(Object root, EvaluationContext context)
			throws Exception {
		List<ExtractedDoc> docs = new ArrayList<ExtractedDoc>();

		for (Object subRoot : getRoots(root, context)) {
			ExtractedDoc extractedDoc = new ExtractedDoc();
			context.setCurrentDoc(extractedDoc);

			extractFields(subRoot, context, extractedDoc);
			insertSpecialFields(context, extractedDoc);
			extractOutlinks(subRoot, context, extractedDoc);

			docs.add(extractedDoc);
		}

		return docs;
	}

	/* Created by Bayan group, replaced by below function - bachtv */
	/*
	 * @SuppressWarnings("unchecked") protected void extractFields(Object
	 * subRoot, EvaluationContext context, ExtractedDoc extractedDoc) throws
	 * Exception { for (ExtractTo extractTo : extractTos) { if
	 * (LOGGER.isDebugEnabled()) LOGGER.debug(extractTo.toString());
	 * List<String> res = extractTo.extract(subRoot, context); if (res != null)
	 * { Field field = extractTo.getField(); if (field.isMulti()) { Object
	 * values = extractedDoc.getField(field.getName()); List<String> fieldValues
	 * = res; if (values != null) { ((List<String>) values).addAll(fieldValues);
	 * } else { List<String> fieldValueList = new ArrayList<String>();
	 * fieldValueList.addAll(fieldValues);
	 * extractedDoc.addField(field.getName(), fieldValueList); } } else {
	 * StringBuilder fieldValue = new StringBuilder(); join(fieldValue, res);
	 * extractedDoc.addField(field.getName(), fieldValue.toString()); } } } }
	 */

	@SuppressWarnings("unchecked")
	protected void extractFields(Object subRoot, EvaluationContext context,
			ExtractedDoc extractedDoc) throws Exception {
		List<String> excludeLinks = new ArrayList<String>();
		String header = "";
		for (ExtractTo extractTo : extractTos) {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug(extractTo.toString());
			List<String> res = extractTo.extract(subRoot, context);
			if (res != null) {
				Field field = extractTo.getField();
				if (field.isMulti()) {
					Object values = extractedDoc.getField(field.getName());
					List<String> fieldValues = res;
					if (values != null) {
						((List<String>) values).addAll(fieldValues);
						if (field.getName().equals("all-links"))
							excludeLinks = fieldValues;
					} else {
						List<String> fieldValueList = new ArrayList<String>();
						fieldValueList.addAll(fieldValues);
						extractedDoc.addField(field.getName(), fieldValueList);
						if (field.getName().equals("all-links"))
							excludeLinks = fieldValues;
					}
				} else {
					StringBuilder fieldValueBuilder = new StringBuilder();
					join(fieldValueBuilder, res);
					String fieldValue = fieldValueBuilder.toString();
					if (field.getName().equals("header"))
						header = fieldValue;
					if (field.getName().equals("all-content"))	
						if (!header.equals(""))
							fieldValue = replaceLast(fieldValue, header, "");
						for (String excludeLink : excludeLinks) {
							if (excludeLink.length() > 1) {								
								fieldValue = replaceLast(fieldValue, excludeLink, "");							
							}
						}
					fieldValue = fieldValue.trim().replaceAll(" +", " ");
//					extractedDoc.addField("content-en", fieldValue);
//					extractedDoc.addField("content-vi", fieldValue);
					extractedDoc.addField(field.getName(), fieldValue);
				}
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void extractOutlinks(Object root, EvaluationContext context,
			ExtractedDoc extractedDoc) throws Exception {
		if (outlinks != null) {
			for (Function linkFunction : outlinks) {
				extractedDoc.getOutlinks().addAll(
						(List) linkFunction.extract(root, context));
			}
		}
	}
	
	public String replaceLast(String string, String substring, String replacement)
	{
	  int index = string.lastIndexOf(substring);
	  if (index == -1)
	    return string;
	  return string.substring(0, index) + replacement
	          + string.substring(index+substring.length());
	}

	protected void insertSpecialFields(EvaluationContext context,
			ExtractedDoc extractedDoc) {
		Object url = extractedDoc.getFields().get(URL_FIELD);
		if (url != null)
			extractedDoc.setUrl(url.toString());
		else
			throw new RuntimeException(
					"Url field for a document or fragment can not be null. Current url: "
							+ context.getContent().getUrl());

		Object title = extractedDoc.getFields().get(TITLE_FIELD);
		if (title != null)
			extractedDoc.setTitle(title.toString());
		else
			extractedDoc.setTitle(url.toString());

		Object text = extractedDoc.getFields().get(TEXT_FIELD);
		if (text != null) {
			extractedDoc.setText(text.toString());
		} else
			extractedDoc.setText("");
	}

	public static void join(StringBuilder res, List<?> items) {
		for (int i = 0; i < items.size(); i++) {
			Object item = items.get(i);
			if (item instanceof List) {
				join(res, (List<?>) item);
			} else
				res.append(item);
			if (i < items.size() - 1)
				res.append(' ');
		}
	}

	/**
	 * @return the outlinks
	 */
	public List<Function> getOutlinks() {
		return outlinks;
	}

}
