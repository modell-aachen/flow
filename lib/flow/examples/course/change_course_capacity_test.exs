defmodule Ariadne.Flow.Examples.Course.ChangeCourseCapacityTest do
  use Ariadne.Flow.Test.Gwt, async: true
  alias Ariadne.Flow.Examples.Course.ChangeCourseCapacity
  alias Ariadne.Flow.Examples.Course.Events

  gwt "Changing course capacity" do
    err("for a non-existing course",
      given: [],
      when: ChangeCourseCapacity.command(%{course_id: "course-1", new_capacity: 25}),
      then: :course_not_found
    )

    err("with no actual change",
      given: [%Events.CourseDefined{course_id: "course-1", capacity: 30}],
      when: ChangeCourseCapacity.command(%{course_id: "course-1", new_capacity: 30}),
      then: :no_change
    )

    ok("for an existing course",
      given: [%Events.CourseDefined{course_id: "course-1", capacity: 30}],
      when: ChangeCourseCapacity.command(%{course_id: "course-1", new_capacity: 25}),
      then: [%Events.CourseCapacityChanged{course_id: "course-1", new_capacity: 25}]
    )
  end
end
