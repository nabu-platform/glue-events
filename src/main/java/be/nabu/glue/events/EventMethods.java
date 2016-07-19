package be.nabu.glue.events;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import be.nabu.glue.ScriptRuntime;
import be.nabu.glue.annotations.GlueMethod;
import be.nabu.glue.api.Lambda;
import be.nabu.glue.api.Script;
import be.nabu.glue.impl.GlueUtils;
import be.nabu.libs.evaluator.annotations.MethodProviderClass;
import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.events.api.EventSubscription;
import be.nabu.libs.events.api.ResponseHandler;
import be.nabu.libs.events.impl.EventDispatcherImpl;

@MethodProviderClass(namespace = "event")
public class EventMethods {
	
	private static Map<String, EventDispatcher> dispatchers = new HashMap<String, EventDispatcher>();
	private static ForkJoinPool pool = new ForkJoinPool();
	
	private static EventDispatcher getDispatcher(String name) {
		if (!dispatchers.containsKey(name)) {
			synchronized(dispatchers) {
				if (!dispatchers.containsKey(name)) {
					dispatchers.put(name, new EventDispatcherImpl());
				}
			}
		}
		return dispatchers.get(name);
	}
	
	@GlueMethod(description = "Subscribe to an event queue")
	public static EventSubscription<Object, Object> subscribe(String queue, Lambda handler, final Lambda selector) {
		EventSubscription<Object, Object> subscription = getDispatcher(queue).subscribe(Object.class, new LambdaEventHandler(handler, ScriptRuntime.getRuntime().fork(true)));
		if (selector != null) {
			final ScriptRuntime runtime = ScriptRuntime.getRuntime();
			subscription.filter(new EventFilterImpl(selector, runtime));
		}
		return subscription;
	}
	
	@GlueMethod(description = "Unsubscribe")
	@SuppressWarnings("rawtypes")
	public static void unsubscribe(EventSubscription subscription) {
		subscription.unsubscribe();
	}
	
	@GlueMethod(description = "Fire an event")
	public static Object fire(final String queue, final Object event, Lambda acceptor) {
		final Script source = ScriptRuntime.getRuntime().getScript();
		final EventDispatcher dispatcher = getDispatcher(queue);
		final ScriptRuntime fork = ScriptRuntime.getRuntime().fork(true);
		if (acceptor != null) {
			return dispatcher.fire(event, source, new LambdaResponseHandler(acceptor, fork));
		}
		else {
			return pool.submit(new Runnable() {
				@Override
				public void run() {
					fork.registerInThread();
					dispatcher.fire(event, source, null, null);
				}
			});
		}
	}
	
	public static final class EventFilterImpl implements EventHandler<Object, Boolean> {
		private final Lambda selector;
		private final ScriptRuntime runtime;

		public EventFilterImpl(Lambda selector, ScriptRuntime runtime) {
			this.selector = selector;
			this.runtime = runtime;
		}

		@Override
		public Boolean handle(Object event) {
			Object calculate = GlueUtils.calculate(selector, runtime, Arrays.asList(event));
			// if no response is given, filter out the event
			if (calculate == null) {
				return true;
			}
			Boolean value = GlueUtils.convert(calculate, Boolean.class);
			// invert value, if you return "true" from the filter, we want to accept the value
			return !value;
		}
	}

	public static class LambdaEventHandler implements EventHandler<Object, Object> {
		private Lambda lambda;
		private ScriptRuntime runtime;
		public LambdaEventHandler(Lambda lambda, ScriptRuntime runtime) {
			this.lambda = lambda;
			this.runtime = runtime;
		}
		@Override
		public Object handle(Object event) {
			return GlueUtils.calculate(lambda, runtime, Arrays.asList(event));
		}
	}
	
	public static class LambdaResponseHandler implements ResponseHandler<Object, Object> {
		private Lambda lambda;
		private ScriptRuntime runtime;
		public LambdaResponseHandler(Lambda lambda, ScriptRuntime runtime) {
			this.lambda = lambda;
			this.runtime = runtime;
		}
		@Override
		public Object handle(Object event, Object response, boolean isLast) {
			return GlueUtils.calculate(lambda, runtime, Arrays.asList(event));
		}
	}
}
