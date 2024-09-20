package com.github.jarol.azure.search.spark.sql.connector.read;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

final class ClassHelper {

    /**
     * Create an instance of a class by detecting a constructor that matches the number and type of provided parameters
     * @param tClass input class
     * @param constructorParameters parameters for class constructor
     * @param <T> class type
     * @return an instance of given class
     * @throws InvocationTargetException if constructor invocation fails
     * @throws InstantiationException if an instance could not be created
     * @throws IllegalAccessException if the constructor cannot be accessed
     * @throws NoSuchMethodException if no matching constructor exists
     */

    static <T> @NotNull T createInstance(
            @NotNull Class<T> tClass,
            Object @NotNull... constructorParameters
    ) throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {

        // Predicate for spotting the proper constructor
        int parametersCount = constructorParameters.length;
        Predicate<Constructor<?>> predicateOnArguments = constructor -> parametersCount == 0 ||
                Arrays.equals(
                        constructor.getParameterTypes(),
                        Arrays.stream(constructorParameters).map(Object::getClass).toArray(Class<?>[]::new)
        );

        // Retrieve the constructor
        Optional<Constructor<?>> optionalConstructor = Arrays.stream(tClass.getConstructors()).filter(
                constructor -> constructor.getParameterCount() == parametersCount &&
                        predicateOnArguments.test(constructor)
        ).findFirst();

        if (optionalConstructor.isPresent()) {
            //noinspection unchecked
            return (T) optionalConstructor.get().newInstance(constructorParameters);
        } else {

            // Create a detailed description
            String constructorDescription = parametersCount == 0 ?
                    "no-args constructor" :
                    String.format("%s arg(s) constructor with type parameters (%s)",
                            parametersCount,
                            Arrays.stream(constructorParameters)
                                    .map(o -> o.getClass().getName())
                                    .collect(Collectors.joining(","))
                    );
            throw new NoSuchMethodException(String.format(
                    "Could not find a %s for class %s",
                    constructorDescription, tClass.getName())
            );
        }
    }
}
