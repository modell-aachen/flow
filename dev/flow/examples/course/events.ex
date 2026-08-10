defmodule Ariadne.Flow.Examples.Course.Events do
  alias __MODULE__
  def course_tag(course_id), do: "course:#{course_id}"
  def student_tag(student_id), do: "student:#{student_id}"

  defmodule CourseDefined do
    @derive {Ariadne.Flow.Event, type: "course-defined"}
    defstruct [:course_id, :capacity]

    def tags(e), do: [Events.course_tag(e.course_id)]
  end

  defmodule CourseCapacityChanged do
    @derive {Ariadne.Flow.Event, type: "course-capacity-changed"}
    defstruct [:course_id, :new_capacity]

    def tags(e), do: [Events.course_tag(e.course_id)]
  end

  defmodule StudentSubscribedToCourse do
    @derive {Ariadne.Flow.Event, type: "student-subscribed-to-course"}
    defstruct [:student_id, :course_id]

    def tags(e), do: [Events.student_tag(e.student_id), Events.course_tag(e.course_id)]
  end
end
