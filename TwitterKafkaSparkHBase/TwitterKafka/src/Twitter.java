public class Twitter {
	
Data data;
	
	public Twitter(Data data) {
		this.data = data;
	}

	public Data getData() {
		return data;
	}

	public void setData(Data data) {
		this.data = data;
	}

	public class Data {
		String id;
		String text;
		
		public Data(String id, String text) {
			this.id = id;
			this.text = text;
		}
	
		public String getId() {
			return id;
		}
		
		public String getText() {
			return text;
		}
	}
	
}
