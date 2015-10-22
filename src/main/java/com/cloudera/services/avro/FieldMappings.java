package com.cloudera.services.avro;

public class FieldMappings {
	private String sourceTable;
	private String sourceColumn;
	private String targetTable;
	private String targetColumn;

	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}

	public String getSourceColumn() {
		return sourceColumn;
	}

	public void setSourceColumn(String sourceColumn) {
		this.sourceColumn = sourceColumn;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public String getTargetColumn() {
		return targetColumn;
	}

	public void setTargetColumn(String targetColumn) {
		this.targetColumn = targetColumn;
	}
}
