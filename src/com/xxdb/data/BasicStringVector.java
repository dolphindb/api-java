package com.xxdb.data;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicStringVector extends AbstractVector{
	static final String[] symbols = new String[]{"A","AA","AABWS","AAC","AACC","AAI","AAME","AANB","AAON","AAP","AAPL",
			"AAPR","AAR","AATI","AAU","AAUKD","AAV","AAWW","AAY","AAZST","AB","ABA","ABAX","ABB","ABBC","ABBI",
			"ABC","ABCB","ABCO","ABCW","ABD","ABER","ABFS","ABG","ABI","ABIX","ABK","ABL","ABM","ABMC","ABMD",
			"ABN","ABNJ","ABNPRE","AB   NPRF","ABNPRG","ABP","ABPI","ABR","ABT","ABTL","ABV","ABVA","ABVC","ABWPRA",
			"ABX","ABXA","ABY","ACA","ACAD","ACAP","ACAS","ACAT","ACBA","ACC","ACCL","ACE","ACEC","ACEL","ACEPRC",
			"ACET","ACF","ACFC","ACG","ACGL","ACGY","ACH","ACHN","ACI","ACIW","ACL","ACLI","ACLS","ACM","ACME",
			"ACMR","ACN","ACO","ACOR","ACP","ACPW","ACS","ACTG","ACTI","ACTL","ACTS","ACTU","ACU","ACUS","ACV",
			"ACW","ACXM","ACY","ADAM","ADAT","ADBE","ADBL","ADC","ADCT","ADEP","ADES","ADF","ADG","ADH","ADI",
			"ADK","ADKWS","ADL","ADLR","ADLS","ADM","ADP","ADPI","ADPT","ADRA","ADRD","ADRE","ADRU","ADS","ADSK",
			"ADST","ADSX","ADTN","ADVNA","ADVNB","ADVS","ADX","ADY","AE","AEA","AEB","AEC","AECPRB","AED","AEE",
			"AEG","AEH","AEHR","AEIS","AEL","AEM","AEMLW","AEN","AEO","AEP","AEPI","AER","AERO","AERT","AES",
			"AESPRC","AET","AETI","AEV","AEY","AEZ","AEZS","AF","AFAM","AFB","AFC","AFCE","AFE","AFF","AFFM",
			"AFFX","AFFY","AFG","AFL","AFN","AFO","AFOP","AFP","AFR","AFSI","AFT","AG","AGC","AGD","AGE","AGEN",
			"AGG","AGII","AGIX","AGL","AGM","AGMA","AGN","AGO","AGP","AGT","AGU","AGYS","AHCI","AHD","AHG","AHGP",
			"AHHPRA","AHII","AHL","AHLPR","AHLPRA","AHM","AHMPRA","AHMPRB","AHN","AHO","AHPI","AHR","AHRPRC","AHRPRD",
			"AHS","AHT","AHTPRA","AHTPRD","AHY","AIB","AID","AIG","ACSEF","AGB","AGIID","ACPPR"};
	public static final java.util.Set<String> symbolSet = new HashSet<>(Arrays.asList(symbols));
	private String[] values;
	private boolean isSymbol;
	
	public BasicStringVector(int size){
		this(DATA_FORM.DF_VECTOR, size, false);
	}
	
	public BasicStringVector(List<String> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new String[list.size()];
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
		this.isSymbol = false;
	}
	
	public BasicStringVector(String[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
		this.isSymbol = false;
	}
	
	protected BasicStringVector(DATA_FORM df, int size, boolean isSymbol){
		super(df);
		values = new String[size];
		this.isSymbol = isSymbol;
	}
	
	protected BasicStringVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int columns = in.readInt();
		int size = rows * columns;
		values = new String[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readString();
	}
	
	public Scalar get(int index){
		return new BasicString(values[index]);
	}
	
	public String getString(int index){
		return values[index];
	}
	
	public void set(int index, Scalar value) throws Exception {
		values[index] = value.getString();
	}
	
	public void setString(int index, String value){
		values[index] = value;
	}
	
	@Override
	public boolean isNull(int index) {
		return values[index] == null || values[index].isEmpty();
	}

	@Override
	public void setNull(int index) {
		values[index] = "";
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.LITERAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return isSymbol ? DATA_TYPE.DT_SYMBOL : DATA_TYPE.DT_STRING;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicString.class;
	}

	@Override
	public int rows() {
		return values.length;
	}	
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for (String str: values)
			out.writeString(str);
	}
}
