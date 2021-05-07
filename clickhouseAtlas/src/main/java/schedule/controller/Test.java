package schedule.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import schedule.service.ScheduleService;


@RestController
@RequestMapping("/test")
public class Test {
	@Autowired
	private ScheduleService scheduleService;
	@RequestMapping(value = "test", method = RequestMethod.GET)
	public void test() {
		scheduleService.clickhouseToAtlas();
	}
}
