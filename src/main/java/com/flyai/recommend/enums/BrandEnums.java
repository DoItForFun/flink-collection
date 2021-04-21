package com.flyai.recommend.enums;

import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.base.Objects;

/**
 * @author lizhe
 */
public enum BrandEnums {
    APPLE(0 , "APPLE"),
    HUAWEI(1 , "HUAWEI"),
    OPPO(2 , "OPPO"),
    XIAOMI(3 , "XIAOMI"),
    VIVO(4 , "VIVO"),
    SAMSUNG(5 , "SAMSUNG"),
    ONEPLUS(6 , "ONEPLUS"),
    MEIZU(7 , "MEIZU"),
    REALME(8 , "REALME"),
    MEITU(9 , "MEITU"),
    SMARTISAN(10 , "SMARTISAN"),
    NUBIA(11 , "NUBIA"),
    BLACKSHARk(12 , "BLACKSHARk"),
    LENOVO(13 , "LENOVO"),
    QIHU(14 , "360"),
    DELTAINNO(15 , "DELTAINNO"),
    GIONEE(16 , "GIONEE"),
    ZTE(17 , "ZTE"),
    SONY(18 , "SONY"),
    GOOGLE(19 , "GOOGLE"),
    OTHER(20 , "OTHER");

    public Integer getIndex() {
        return index;
    }

    public String getBrand() {
        return brand;
    }

    private final Integer index;
    private final String brand;

    public static Integer getIndexByBrand(String brand) {
        for (BrandEnums brandString : BrandEnums.values()) {
            if(Objects.equal(brandString.getBrand() , brand.toUpperCase())){
                return brandString.getIndex();
            }
        }
        return OTHER.getIndex();
    }

    public static Integer getTotalBrands(){
        return BrandEnums.values().length;
    }

    BrandEnums(Integer index , String brand){
        this.index = index;
        this.brand = brand;
    }


}
