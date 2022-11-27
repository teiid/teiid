package org.teiid.translator.document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.Join.JoinType;

class DocumentJoinNode extends DocumentNode {
    private DocumentNode left;
    private DocumentNode right;
    private JoinType type;

    public DocumentJoinNode(DocumentNode left, JoinType joinType, DocumentNode right) {
        this.left = left;
        this.right = right;
        this.type = joinType;
    }

    public List<Map<String, Object>> mergeTuples(List<Map<String, Object>> leftRows,
             Document parentDocument) {
        List<Map<String, Object>> joinedRows = new ArrayList<Map<String,Object>>();
        List<? extends Document> rightDocuments = parentDocument
                .getChildDocuments(this.right.getName());
        if (rightDocuments == null) {
            if (this.type.equals(JoinType.LEFT_OUTER_JOIN)) {
                for (Map<String, Object> leftRow:leftRows) {
                    LinkedHashMap<String, Object> row = new LinkedHashMap<String, Object>();
                    row.putAll(leftRow);
                    joinedRows.add(row);
                }
                if (this.joinNode != null) {
                    // do further joins, only span up to sibiling or child. In this case
                    // there can be only sibiling
                    joinedRows = this.joinNode.mergeTuples(joinedRows, parentDocument);
                }
            }
        } else {
            for (Map<String, Object> leftRow:leftRows) {
                for (Document rightDocument : rightDocuments) {
                    Map<String, Object> rightRow = rightDocument.getProperties();
                    LinkedHashMap<String, Object> row = new LinkedHashMap<String, Object>();
                    if (this.type.equals(JoinType.INNER_JOIN)) {
                        if (rightRow != null && !rightRow.isEmpty()) {
                            row.putAll(leftRow);
                            row.putAll(rightRow);
                        }
                    } else if (this.type.equals(JoinType.LEFT_OUTER_JOIN)) {
                        row.putAll(leftRow);
                        if (rightRow != null && !rightRow.isEmpty()) {
                            row.putAll(rightRow);
                        }
                    }
                    joinedRows.add(row);
                }

                if (this.joinNode != null) {
                    // do further joins, only span up to sibiling or child
                    for (Document rightDocument : rightDocuments) {
                        joinedRows = this.joinNode.mergeTuples(joinedRows, rightDocument);
                    }
                }
            }
        }
        return joinedRows;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.left);
        sb.append(this.type);
        sb.append(this.right);
        return sb.toString();
    }
}
