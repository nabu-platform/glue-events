package be.nabu.glue.events;

import java.util.ArrayList;
import java.util.List;

import be.nabu.glue.api.StaticMethodFactory;

public class EventStaticMethodFactory implements StaticMethodFactory {

	@Override
	public List<Class<?>> getStaticMethodClasses() {
		List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(EventMethods.class);
		return classes;
	}
	
}
