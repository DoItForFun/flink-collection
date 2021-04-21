package com.flyai.recommend.enums;

import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.base.Objects;

/**
 * @author lizhe
 */
public enum ConstellationEnums {
    Ari(0 , "Ari"),
    Tau(1 , "Tau"),
    Gem(2 , "Gem"),
    Can(3 , "Can"),
    Leo(4 , "Leo"),
    Vir(5 , "Vir"),
    Lib(6 , "Lib"),
    Sco(7 , "Sco"),
    Sag(8 , "Sag"),
    Cap(9 , "Cap"),
    Aqu(10 , "Aqu"),
    Pis(11 , "Pis"),
    Unknown(12 , "Unknown");

    public Integer getIndex() {
        return index;
    }

    public String getConstellation() {
        return Constellation;
    }

    private final Integer index;
    private final String Constellation;


    ConstellationEnums(Integer index , String constellation){
        this.index = index;
        this.Constellation = constellation;
    }

    public static Integer getIndexByConstellation(String constellation) {

        for (ConstellationEnums constellationEnums : ConstellationEnums.values()) {
            if(Objects.equal(constellationEnums.getConstellation() , constellation)){
                return constellationEnums.getIndex();
            }
        }
        return Unknown.getIndex();
    }

    public static Integer getTotalConstellation(){
        return ConstellationEnums.values().length;
    }
}
