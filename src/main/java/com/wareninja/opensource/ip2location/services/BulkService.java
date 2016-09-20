package com.wareninja.opensource.ip2location.services;

import java.util.List;

public interface BulkService {

    void proceed(List content);

    void dropDataSet();

    void close();
}
