#include "handler.h"

using namespace std;


int main(int argc, char* argv[])
{
    cout << "Starting up.." << endl;
    sc::kafka vms("10.0.105.7:9092");
#ifdef NDEBUG
    vms.MaxRecords(0);
    vms.Poll(200);
    vms.PollAtEnd(1000);
    vms.AddTopic("alarm_as_topic");
    vms.AddTopic("alarm_message_topic");
    vms.AddTopic("alarm_only_topic");
    vms.AddTopic("device_media_enevt_topic");
    vms.AddTopic("device_register_topic");
    vms.AddTopic("device_upgrade_result_topic");
    vms.AddTopic("heartbeat_topic");
    vms.AddTopic("location_status_topic");
    vms.AddTopic("location_topic");
    vms.AddTopic("rule_engine_topic");
    vms.AddTopic("ta_clistener_topic");
    vms.AddTopic("media_file_topic");
    // vms.AddTopic("terminal_upload_topic");
    vms.AddTopic("vehicle_alarm_topic");

#else
    vms.MaxRecords(10);
    vms.Poll(200);
    vms.FromBeginning(false);
    // vms.AddTopic("alarm_as_topic");
    // vms.AddTopic("alarm_message_topic");
    // vms.AddTopic("alarm_only_topic");
    // vms.AddTopic("device_media_enevt_topic");
    // vms.AddTopic("device_register_topic");
    // vms.AddTopic("device_upgrade_result_topic");
    // vms.AddTopic("heartbeat_topic");
    // vms.AddTopic("location_status_topic");
    // vms.AddTopic("location_topic");
    // vms.AddTopic("rule_engine_topic");
    vms.AddTopic("ta_clistener_topic");
    // vms.AddTopic("media_file_topic");
    // vms.AddTopic("terminal_upload_topic");
    // vms.AddTopic("vehicle_alarm_topic");
#endif

    vms.consume(Handler);
    cout << "Done" << endl;
}
