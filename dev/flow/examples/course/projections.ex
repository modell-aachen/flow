defmodule Ariadne.Flow.Examples.Course.Projections do
  alias Ariadne.Flow.Examples.Course.Events
  alias Ariadne.Flow.Projection

  def course_exists(course_id) do
    Projection.new(
      %{
        filter: %{
          types: [Events.CourseDefined],
          tags: [Events.course_tag(course_id)],
          only_last_event: true
        },
        initial_state: false
      },
      fn _state, %Events.CourseDefined{}, _metadata -> true end
    )
  end

  def course_capacity(course_id) do
    Projection.new(
      %{
        filter: %{
          types: [Events.CourseDefined, Events.CourseCapacityChanged],
          tags: [Events.course_tag(course_id)],
          only_last_event: true
        },
        initial_state: 0
      },
      fn
        _state, %Events.CourseDefined{capacity: capacity}, _metadata -> capacity
        _, %Events.CourseCapacityChanged{new_capacity: new_capacity}, _metadata -> new_capacity
      end
    )
  end
end
