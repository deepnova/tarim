package org.deepexi;

import java.io.Serializable;
import java.util.List;

public class WResult implements Serializable {
    public WResult(){}
    public List<String> strList;

    public WResult(List<String> strList) {
        this.strList = strList;
    }


}
